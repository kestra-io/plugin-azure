package io.kestra.plugin.azure.eventhubs.service.consumer;

import com.azure.messaging.eventhubs.CheckpointStore;
import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventProcessorClient;
import com.azure.messaging.eventhubs.models.Checkpoint;
import com.azure.messaging.eventhubs.models.EventBatchContext;
import com.azure.messaging.eventhubs.models.EventPosition;
import com.azure.messaging.eventhubs.models.PartitionContext;
import io.kestra.plugin.azure.eventhubs.client.EventHubClientFactory;
import io.kestra.plugin.azure.eventhubs.config.EventHubConsumerConfig;
import io.kestra.plugin.azure.eventhubs.model.EventDataObject;
import io.kestra.plugin.azure.eventhubs.service.EventDataObjectConverter;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Slf4j
public final class EventHubConsumerService {

    private final EventHubClientFactory clientFactory;
    private final EventHubConsumerConfig config;
    private final EventDataObjectConverter converter;
    private final CheckpointStore checkpointStore;

    /**
     * Creates a new {@link EventHubConsumerService} instance.
     *
     * @param clientFactory   The {@link EventHubClientFactory} - Cannot be {@code null}.
     * @param consumerConfig  The {@link EventHubConsumerConfig} - Cannot be {@code null}.
     * @param converter       The {@link EventDataObjectConverter} to be used for converting entities to event data.
     * @param checkpointStore The {@link CheckpointStore}.
     */
    public EventHubConsumerService(final EventHubClientFactory clientFactory,
                                   final EventHubConsumerConfig consumerConfig,
                                   final EventDataObjectConverter converter,
                                   final CheckpointStore checkpointStore) {
        this.clientFactory = Objects.requireNonNull(clientFactory, "clientFactory cannot be null");
        this.config = Objects.requireNonNull(consumerConfig, "consumerConfig cannot be null");
        this.converter = Objects.requireNonNull(converter, "converter cannot be null");
        this.checkpointStore = Objects.requireNonNull(checkpointStore, "checkpointStoreSupplier cannot be null");
    }

    public Map<EventHubNamePartition, Integer> poll(final ConsumerContext consumerContext,
                                                    final EventProcessorListener listener) throws Exception {

        Logger logger = consumerContext.logger();

        CountDownLatch latch = new CountDownLatch(1);

        // Create Map that will hold all initialized partitions.
        Set<String> partitions = Collections.synchronizedSet(new HashSet<>());

        // Counter
        Map<EventHubNamePartition, AtomicInteger> eventsByEventHubNamePartition = new ConcurrentHashMap<>();

        Map<EventHubNamePartition, Checkpoint> checkpointsByPartitions = new ConcurrentHashMap<>();

        // Create single EventProcessorClient.
        EventProcessorClient client = clientFactory.createEventProcessorClientBuilder(config)
            .consumerGroup(config.consumerGroup())
            .checkpointStore(checkpointStore)
            // Set the offset reset strategy
            .initialPartitionEventPosition(partition -> {
                EventPosition position = config.partitionStartingPosition();
                if (logger.isInfoEnabled()) {
                    logger.info("Initializing partitionId {} with offset={}, sequenceNumber={}, enqueuedDateTime={} if no checkpoint exist.",
                        partition,
                        position.getOffset(),
                        position.getSequenceNumber(),
                        position.getEnqueuedDateTime()
                    );
                }
                return position;
            })
            // Capture the partition to process.
            .processPartitionInitialization(context -> {
                partitions.add(context.getPartitionContext().getPartitionId());
            })
            // Process Events
            .processEventBatch(context -> {
                PartitionContext partitionContext = context.getPartitionContext();
                if (!partitions.remove(partitionContext.getPartitionId())) {
                    if (logger.isTraceEnabled()) {
                        logger.trace(
                            "PartitionId={} has already been consumed once. Rejecting events.",
                            partitionContext.getPartitionId()
                        );
                    }
                    return;
                }
                List<EventData> events = context.getEvents();

                // Convert eventData, and invoke listener.
                for (EventDataObject event : converter.convertFromEventData(events)) {
                    try {
                        listener.onEvent(event, partitionContext);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }

                EventHubNamePartition key = new EventHubNamePartition(
                    partitionContext.getEventHubName(),
                    partitionContext.getPartitionId()
                );

                // Keep checkpoint of the last event in the batch
                createCheckpoint(context)
                    .ifPresent(checkpoint -> checkpointsByPartitions.put(key, checkpoint));

                // Increment event counter for the current partition.
                eventsByEventHubNamePartition
                    .computeIfAbsent(key, ignored -> new AtomicInteger(0))
                    .addAndGet(events.size());

                // Check whether all partitions were polled at-least once.
                if (partitions.isEmpty()) {
                    // Proactively stop consuming.
                    latch.countDown();
                }

            }, consumerContext.maxPollEvents(), consumerContext.maxBatchPartitionWait())
            // Handle errors
            .processError(errorContext -> {
                PartitionContext partitionContext = errorContext.getPartitionContext();
                logger.error("Failed to process eventHub: {}, partitionId: {} with consumerGroup: {}",
                    partitionContext.getEventHubName(),
                    partitionContext.getPartitionId(),
                    partitionContext.getConsumerGroup(),
                    errorContext.getThrowable());
                latch.countDown(); // stop processing immediately.
            })
            .buildEventProcessorClient();

        try {
            client.start();
            if (!latch.await(consumerContext.maxDuration().toMillis(), TimeUnit.MILLISECONDS)) {
                logger.debug("Reached `maxDuration`({}ms) before receiving events from EventHub {}.",
                    consumerContext.maxDuration().toMillis(),
                    config.eventHubName());
            }
        } finally {
            client.stop();
            listener.onStop();
            updateCheckpoints(checkpointStore, checkpointsByPartitions.values(), logger);
        }

        return eventsByEventHubNamePartition
            .entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, it -> it.getValue().get()));
    }

    private void updateCheckpoints(CheckpointStore store,
                                   Collection<Checkpoint> checkpoints,
                                   Logger logger) {
        for (Checkpoint checkpoint : checkpoints) {
            logger.debug("Checkpointing position for consumerGroup={}, eventHubName={}, partitionId={}, sequenceNumber={}, and offset={}.",
                checkpoint.getConsumerGroup(),
                checkpoint.getEventHubName(),
                checkpoint.getPartitionId(),
                checkpoint.getSequenceNumber(),
                checkpoint.getOffset()
            );
            store.updateCheckpoint(checkpoint).block();
        }
    }

    private Optional<Checkpoint> createCheckpoint(final EventBatchContext context) {
        List<EventData> events = context.getEvents();
        if (events.isEmpty()) {
            return Optional.empty();
        }
        PartitionContext partitionContext = context.getPartitionContext();

        return Optional.of(new Checkpoint()
            .setFullyQualifiedNamespace(partitionContext.getFullyQualifiedNamespace())
            .setEventHubName(partitionContext.getEventHubName())
            .setConsumerGroup(partitionContext.getConsumerGroup())
            .setPartitionId(partitionContext.getPartitionId())
            .setSequenceNumber(events.get(events.size() - 1).getSequenceNumber())
            .setOffset(events.get(events.size() - 1).getOffset())
        );
    }

    public interface EventProcessorListener {

        /**
         * Invokes on each received event.
         *
         * @param event The event to be processed.
         */
        void onEvent(EventDataObject event, PartitionContext context) throws Exception;

        /**
         * Invokes when the event processor is stopped.
         */
        default void onStop() throws Exception {
        }
    }
}
