package io.kestra.plugin.azure.eventhubs.service.producer;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventDataBatch;
import com.azure.messaging.eventhubs.EventHubProducerAsyncClient;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.azure.eventhubs.client.EventHubClientFactory;
import io.kestra.plugin.azure.eventhubs.config.EventHubClientConfig;
import io.kestra.plugin.azure.eventhubs.config.EventHubConsumerConfig;
import io.kestra.plugin.azure.eventhubs.model.EventDataObject;
import io.kestra.plugin.azure.eventhubs.service.EventDataObjectConverter;
import org.slf4j.Logger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Default service for sending batches of events into Azure Event Hubs.
 */
public class EventHubProducerService {

    private static final int DEFAULT_MAX_EVENT_PER_BATCH = Integer.MAX_VALUE;
    private final EventHubClientFactory clientFactory;
    private final EventHubClientConfig config;
    private final EventDataObjectConverter adapter;
    private final EventDataBatchFactory batchFactory;

    /**
     * Creates a new {@link EventHubProducerService} instance.
     *
     * @param clientFactory The {@link EventHubClientFactory} - Cannot be {@code null}.
     * @param config        The {@link EventHubConsumerConfig} - Cannot be {@code null}.
     * @param converter     The {@link EventDataObjectConverter} to be used for converting entities to event data.
     */
    public EventHubProducerService(final EventHubClientFactory clientFactory,
                                   final EventHubClientConfig config,
                                   final EventDataObjectConverter converter,
                                   final EventDataBatchFactory batchFactory) {
        this.clientFactory = Objects.requireNonNull(clientFactory, "clientFactory cannot be null");
        this.config = Objects.requireNonNull(config, "config cannot be null");
        this.adapter = Objects.requireNonNull(converter, "converter cannot be null");
        this.batchFactory = Objects.requireNonNull(batchFactory, "dataBatchFactory cannot be null");
    }

    /**
     * Publishes the given event stream as events into Event Hubs.
     *
     * @param eventStream The stream of events to send.
     * @return The sender result.
     */
    public Result sendEvents(BufferedReader eventStream, ProducerContext context) throws IllegalVariableEvaluationException, IOException {
        Flux<EventDataObject> flowable = FileSerde.readAll(eventStream, EventDataObject.class);
        try (EventHubProducerAsyncClient producer = clientFactory.createAsyncProducerClient(config)) {
            return sendEvents(producer, adapter, flowable, context);
        }
    }

    private Result sendEvents(EventHubProducerAsyncClient producer,
                              EventDataObjectConverter adapter,
                              Flux<EventDataObject> flowable,
                              ProducerContext context
    ) {

        Logger logger = context.logger();

        Integer maxEventsPerBatch = Optional
            .ofNullable(context.maxEventsPerBatch())
            .orElse(DEFAULT_MAX_EVENT_PER_BATCH);

        final AtomicInteger numSentEvents = new AtomicInteger(0);
        EventDataBatch firstBatch = batchFactory.createBatch(producer).block();
        final AtomicReference<EventDataBatch> currentBatch = new AtomicReference<>(firstBatch);

        Integer numSentBatches = flowable.flatMap(data -> {
                EventDataBatch batch = currentBatch.get();
                final EventData event = adapter.convertToEventData(data);
                // Set default content-type
                Optional.ofNullable(context.bodyContentType())
                    .ifPresent(event::setContentType);
                // Set default properties
                Optional.ofNullable(context.eventProperties())
                    .ifPresent(props -> event.getProperties().putAll(props));

                int batchSizeInEvents = batch.getCount();
                boolean isFull = batchSizeInEvents >= maxEventsPerBatch;

                if (!isFull && batch.tryAdd(event)) {
                    return Mono.empty();
                }

                // Send the current batch then create another size-limited EventDataBatch
                // and try to fit the event into this new batch.
                int batchSize = batch.getCount();
                int totalSent = numSentEvents.getAndAccumulate(batchSize, Integer::sum);
                if (logger.isTraceEnabled()) {
                    logger.trace("Sending new batch of {} events (total-sent-events: {})", batchSize, totalSent);
                }
                return Mono.when(
                    producer.send(batch),
                    batchFactory.createBatch(producer)
                        .map(newBatch -> {
                            currentBatch.set(newBatch);
                            // Try to add the event that did not fit in the previous
                            // batch into a new empty one.
                            if (!newBatch.tryAdd(event)) {
                                throw new IllegalArgumentException(String.format(
                                    "Event is too large for an empty batch. Max size: %s. Event size: %s",
                                    newBatch.getMaxSizeInBytes(), event.getBodyAsBinaryData().getLength()));
                            }
                            return newBatch;
                        })
                ).then(Mono.just(1));
            })
            .reduce(Integer::sum)
            .block();

        // Eventually send last partial batch.
        final EventDataBatch batch = currentBatch.getAndSet(null);
        if (batch != null && batch.getCount() > 0) {
            int batchSize = batch.getCount();
            int totalSent = numSentEvents.getAndAccumulate(batchSize, Integer::sum);
            if (logger.isTraceEnabled()) {
                logger.trace("Sending new batch of {} events (total-sent-events: {})", batchSize, totalSent);
            }
            producer.send(batch).block();
        }

        numSentBatches = Optional
            .ofNullable(numSentBatches)
            .map(count -> count + 1)
            .orElse(1); // numSentBatches will be NULL when a single and partial batch is sent.

        return new Result(numSentEvents.get(), numSentBatches);
    }

    public record Result(int totalSentEvents, int totalSentBatches) {
    }
}
