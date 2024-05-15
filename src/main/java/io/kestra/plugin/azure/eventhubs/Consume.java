package io.kestra.plugin.azure.eventhubs;

import com.azure.messaging.eventhubs.CheckpointStore;
import com.azure.messaging.eventhubs.checkpointstore.blob.BlobCheckpointStore;
import com.azure.messaging.eventhubs.models.PartitionContext;
import com.azure.storage.blob.BlobContainerAsyncClient;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.azure.eventhubs.client.EventHubClientFactory;
import io.kestra.plugin.azure.eventhubs.config.BlobContainerClientConfig;
import io.kestra.plugin.azure.eventhubs.config.EventHubConsumerConfig;
import io.kestra.plugin.azure.eventhubs.model.EventDataObject;
import io.kestra.plugin.azure.eventhubs.model.EventDataOutput;
import io.kestra.plugin.azure.eventhubs.serdes.Serde;
import io.kestra.plugin.azure.eventhubs.serdes.Serdes;
import io.kestra.plugin.azure.eventhubs.service.EventDataObjectConverter;
import io.kestra.plugin.azure.eventhubs.service.consumer.ConsumerContext;
import io.kestra.plugin.azure.eventhubs.service.consumer.EventHubConsumerService;
import io.kestra.plugin.azure.eventhubs.service.consumer.EventHubNamePartition;
import io.kestra.plugin.azure.eventhubs.service.consumer.StartingPosition;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import reactor.core.publisher.Flux;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The {@link RunnableTask} can be used for consuming batches of events from Azure Event Hubs.
 */
@Plugin(examples = {
    @Example(
        title = "Consume data events from Azure EventHubs.",
        full = true,
        code = {
            """
                id: ConsumeDataEventsFromAzureEventHubs
                namespace: company.team
                tasks:
                - id: consumeFromEventHubs
                  type: io.kestra.plugin.azure.eventhubs.Consume
                  eventHubName: my-eventhub
                  namespace: my-eventhub-namespace
                  connectionString: "{{ secret('EVENTHUBS_CONNECTION') }}"
                  bodyDeserializer: JSON
                  consumerGroup: "$Default"
                  checkpointStoreProperties:
                    containerName: kestra
                    connectionString: "{{ secret('BLOB_CONNECTION') }}"
                """
        }
    )
})
@Schema(
    title = "Consume events from Azure Event Hubs."
)
@Slf4j
@SuperBuilder
@NoArgsConstructor
@Getter
@ToString
@EqualsAndHashCode
public class Consume extends AbstractEventHubTask implements EventHubConsumerInterface, RunnableTask<Consume.Output> {
    // TASK'S PARAMETERS
    @Builder.Default
    private Serdes bodyDeserializer = Serdes.STRING;

    @Builder.Default
    private Map<String, Object> bodyDeserializerProperties = Collections.emptyMap();

    @Builder.Default
    private String consumerGroup = "$Default";

    @Builder.Default
    private StartingPosition partitionStartingPosition = StartingPosition.EARLIEST;

    private String EnqueueTime;

    @Builder.Default
    private Integer maxBatchSizePerPartition = 50;

    @Builder.Default
    private Duration maxWaitTimePerPartition = Duration.ofSeconds(5);

    @Builder.Default
    private Duration maxDuration = Duration.ofSeconds(10);

    @Builder.Default
    private Map<String, String> checkpointStoreProperties = Collections.emptyMap();

    // SERVICES
    @Getter(AccessLevel.NONE)
    @Builder.Default
    private final EventHubClientFactory clientFactory = new EventHubClientFactory();

    /**
     * {@inheritDoc}
     **/
    @Override
    public Output run(RunContext runContext) throws Exception {
        return run(runContext, this);
    }

    /**
     * Runs the consumer task using the specified context and plugin interface.
     *
     * @param runContext The context.
     * @param task       The plugin interface.
     * @return The output.
     * @throws Exception if something wrong happens.
     */
    Output run(RunContext runContext, EventHubConsumerInterface task) throws Exception {

        final EventHubConsumerConfig config = new EventHubConsumerConfig(runContext, task);

        // Create converter
        Serdes serdes = task.getBodyDeserializer();
        Serde serde = serdes.create(task.getBodyDeserializerProperties());
        EventDataObjectConverter converter = new EventDataObjectConverter(serde);

        final EventHubConsumerService service = new EventHubConsumerService(
            clientFactory,
            config,
            converter,
            getBlobCheckpointStore(runContext, task, clientFactory)
        );

        File tempFile = runContext.tempFile(".ion").toFile();
        try (
            BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(tempFile))
        ) {

            final AtomicReference<URI> uri = new AtomicReference<>();

            Logger contextLogger = runContext.logger();

            final ConsumerContext consumerContext = new ConsumerContext(
                task.getMaxBatchSizePerPartition(),
                task.getMaxWaitTimePerPartition(),
                task.getMaxDuration(),
                contextLogger
            );

            Map<EventHubNamePartition, Integer> result = service.poll(
                consumerContext,
                new EventHubConsumerService.EventProcessorListener() {
                    @Override
                    public void onEvent(EventDataObject event, PartitionContext context) throws Exception {
                        if (contextLogger.isTraceEnabled()) {
                            contextLogger.trace(
                                "Received new event from eventHub {} and partitionId={} [offset={}, sequenceId={}]",
                                context.getEventHubName(),
                                context.getPartitionId(),
                                event.offset(),
                                event.sequenceNumber()
                            );
                        }
                        FileSerde.write(output, event);
                    }

                    @Override
                    public void onStop() throws Exception {
                        output.flush();
                        output.close();
                        contextLogger.debug("Copying data to storage.");
                        uri.set(runContext.storage().putFile(tempFile));
                        contextLogger.debug("Copy on storage completed.");

                    }
                });

            int numEvents = result.entrySet().stream()
                .peek(entry -> {
                    Counter counter = Counter.of(
                        "records",
                        entry.getValue(),
                        "eventHubName",
                        entry.getKey().eventHubName(),
                        "partitionId",
                        entry.getKey().partitionId()
                    );
                    runContext.metric(counter);
                })
                .map(Map.Entry::getValue)
                .reduce(Integer::sum)
                .orElse(0);

            return new Output(numEvents, uri.get());
        }
    }

    public Publisher<EventDataOutput> stream(RunContext runContext, EventHubConsumerInterface task) throws Exception {
        final EventHubConsumerConfig config = new EventHubConsumerConfig(runContext, task);

        // Create converter
        Serdes serdes = task.getBodyDeserializer();
        Serde serde = serdes.create(task.getBodyDeserializerProperties());
        EventDataObjectConverter converter = new EventDataObjectConverter(serde);

        final EventHubConsumerService service = new EventHubConsumerService(
            clientFactory,
            config,
            converter,
            getBlobCheckpointStore(runContext, task, clientFactory)
        );

        return Flux.create(
            fluxSink -> {
                Logger contextLogger = runContext.logger();

                final ConsumerContext consumerContext = new ConsumerContext(
                    task.getMaxBatchSizePerPartition(),
                    task.getMaxWaitTimePerPartition(),
                    task.getMaxDuration(),
                    contextLogger
                );

                try {
                    Map<EventHubNamePartition, Integer> result = service.poll(
                        consumerContext,
                        new EventHubConsumerService.EventProcessorListener() {
                            @Override
                            public void onEvent(EventDataObject event, PartitionContext context) {
                                if (contextLogger.isTraceEnabled()) {
                                    contextLogger.trace(
                                        "Received new event from eventHub {} and partitionId={} [offset={}, sequenceId={}]",
                                        context.getEventHubName(),
                                        context.getPartitionId(),
                                        event.offset(),
                                        event.sequenceNumber()
                                    );
                                }

                                fluxSink.next(EventDataOutput.of(event));
                            }

                            @Override
                            public void onStop() {
                                fluxSink.complete();
                            }
                        });

                    result.forEach((key, value) -> {
                        Counter counter = Counter.of(
                            "records",
                            value,
                            "eventHubName",
                            key.eventHubName(),
                            "partitionId",
                            key.partitionId()
                        );
                        runContext.metric(counter);
                    });
                } catch (Exception throwable) {
                    fluxSink.error(throwable);
                }
            });
    }

    private CheckpointStore getBlobCheckpointStore(final RunContext runContext,
                                                   final EventHubConsumerInterface pluginConfig,
                                                   final EventHubClientFactory factory) throws IllegalVariableEvaluationException {
        BlobContainerClientInterface config = BlobContainerClientInterface.builder()
            .containerName(pluginConfig.getCheckpointStoreProperties().get("containerName"))
            .connectionString(pluginConfig.getCheckpointStoreProperties().get("connectionString"))
            .sharedKeyAccountAccessKey(pluginConfig.getCheckpointStoreProperties().get("sharedKeyAccountAccessKey"))
            .sharedKeyAccountName(pluginConfig.getCheckpointStoreProperties().get("sharedKeyAccountName"))
            .build();
        BlobContainerAsyncClient client = factory.createBlobContainerAsyncClient(
            new BlobContainerClientConfig(runContext, config)
        );
        return new BlobCheckpointStore(client);
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Number of events consumed from Azure Event Hubs."
        )
        private final Integer eventsCount;

        @Schema(
            title = "URI of a kestra internal storage file containing the messages."
        )
        private URI uri;
    }
}
