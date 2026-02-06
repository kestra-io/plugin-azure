package io.kestra.plugin.azure.eventhubs;

import com.azure.messaging.eventhubs.CheckpointStore;
import com.azure.messaging.eventhubs.checkpointstore.blob.BlobCheckpointStore;
import com.azure.messaging.eventhubs.models.PartitionContext;
import com.azure.storage.blob.BlobContainerAsyncClient;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.azure.eventhubs.client.EventHubClientFactory;
import io.kestra.plugin.azure.eventhubs.config.BlobContainerClientConfig;
import io.kestra.plugin.azure.eventhubs.config.EventHubConsumerConfig;
import io.kestra.plugin.azure.eventhubs.model.EventDataObject;
import io.kestra.plugin.azure.eventhubs.serdes.Serde;
import io.kestra.plugin.azure.eventhubs.serdes.Serdes;
import io.kestra.plugin.azure.eventhubs.service.EventDataObjectConverter;
import io.kestra.plugin.azure.eventhubs.service.consumer.ConsumerContext;
import io.kestra.plugin.azure.eventhubs.service.consumer.EventHubConsumerService;
import io.kestra.plugin.azure.eventhubs.service.consumer.EventHubNamePartition;
import io.kestra.plugin.azure.eventhubs.service.consumer.StartingPosition;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;


import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The {@link RunnableTask} can be used for consuming batches of events from Azure Event Hubs.
 */

@Plugin(
    examples = {
        @Example(
            title = "Consume data events from Azure EventHubs.",
            full = true,
            code = """
                id: azure_eventhubs_consume_data_events
                namespace: company.team

                tasks:
                  - id: consume_from_eventhub
                    type: io.kestra.plugin.azure.eventhubs.Consume
                    eventHubName: my_eventhub
                    namespace: my_eventhub_namespace
                    connectionString: "{{ secret('EVENTHUBS_CONNECTION') }}"
                    bodyDeserializer: JSON
                    consumerGroup: "$Default"
                    checkpointStoreProperties:
                      containerName: kestra
                      connectionString: "{{ secret('BLOB_CONNECTION') }}"
                """
        )
    },
    metrics = {
        @Metric(name = "records.consumed", type = Counter.TYPE, description = "The total number of events consumed.")
    }
)
@Schema(
    title = "Consume events from Azure Event Hubs",
    description = "Polls Event Hubs partitions in batches, checkpoints to Azure Blob Storage, and writes events to internal storage as Ion. Defaults: consumerGroup=$Default, partitionStartingPosition=EARLIEST, maxBatchSizePerPartition=50, maxWaitTimePerPartition=PT5S, maxDuration=PT10S. Requires checkpointStoreProperties.connectionString and .containerName."
)
@SuperBuilder
@NoArgsConstructor
@Getter
@ToString
@EqualsAndHashCode
public class Consume extends AbstractEventHubTask implements EventHubConsumerInterface, EventHubBatchConsumerInterface, RunnableTask<Consume.Output> {
    // TASK'S PARAMETERS
    @Builder.Default
    @Schema(title = "Body deserializer", description = "Serde used to decode event bodies; defaults to STRING")
    private Property<Serdes> bodyDeserializer = Property.ofValue(Serdes.STRING);

    @Builder.Default
    @Schema(title = "Deserializer properties", description = "Key/value options passed to the selected serde")
    private Property<Map<String, Object>> bodyDeserializerProperties = Property.ofValue(new HashMap<>());

    @Builder.Default
    @Schema(title = "Consumer group", description = "Event Hubs consumer group name; defaults to $Default")
    private Property<String> consumerGroup = Property.ofValue("$Default");

    @Builder.Default
    @Schema(title = "Starting position", description = "Initial position strategy per partition; defaults to EARLIEST")
    private Property<StartingPosition> partitionStartingPosition = Property.ofValue(StartingPosition.EARLIEST);

    @Schema(title = "Start from enqueue time", description = "Optional enqueue time filter (ISO-8601); overrides starting position when set")
    private Property<String> enqueueTime;

    @Builder.Default
    @Schema(title = "Max batch size per partition", description = "Maximum events pulled per partition read; defaults to 50")
    private Property<Integer> maxBatchSizePerPartition = Property.ofValue(50);

    @Builder.Default
    @Schema(title = "Max wait per partition", description = "Maximum wait for a partition batch before returning; defaults to PT5S")
    private Property<Duration> maxWaitTimePerPartition = Property.ofValue(Duration.ofSeconds(5));

    @Builder.Default
    @Schema(title = "Overall max duration", description = "Stop consuming after this duration; defaults to PT10S")
    private Property<Duration> maxDuration = Property.ofValue(Duration.ofSeconds(10));

    @NotNull
    @Schema(title = "Checkpoint store properties", description = "Blob container config for checkpoints (connectionString, containerName required)")
    private Property<Map<String, String>> checkpointStoreProperties;

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
    <T extends EventHubConsumerInterface & EventHubBatchConsumerInterface> Output run(RunContext runContext, T task) throws Exception {

        final EventHubConsumerService service = newEventHubConsumerService(runContext, task);
        final EventDataObjectConverter converter = newConverter(task, runContext);

        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        try (
            BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(tempFile))
        ) {

            final AtomicReference<URI> uri = new AtomicReference<>();

            Logger contextLogger = runContext.logger();

            final ConsumerContext consumerContext = new ConsumerContext(
                runContext.render(task.getMaxBatchSizePerPartition()).as(Integer.class).orElseThrow(),
                runContext.render(task.getMaxWaitTimePerPartition()).as(Duration.class).orElse(null),
                runContext.render(task.getMaxDuration()).as(Duration.class).orElse(null),
                converter,
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
                        "records.consumed",
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

    public EventDataObjectConverter newConverter(final EventHubConsumerInterface task, RunContext runContext) throws IllegalVariableEvaluationException {
        Serdes serdes = runContext.render(task.getBodyDeserializer()).as(Serdes.class).orElse(null);
        Serde serde = serdes.create(runContext.render(task.getBodyDeserializerProperties()).asMap(String.class, Object.class));
        return new EventDataObjectConverter(serde);
    }

    public EventHubConsumerService newEventHubConsumerService(final RunContext runContext,
                                                              final EventHubConsumerInterface task) throws IllegalVariableEvaluationException {
        return new EventHubConsumerService(
            clientFactory,
            new EventHubConsumerConfig(runContext, task),
            getBlobCheckpointStore(runContext, task, clientFactory)
        );
    }

    private CheckpointStore getBlobCheckpointStore(final RunContext runContext,
                                                   final EventHubConsumerInterface pluginConfig,
                                                   final EventHubClientFactory factory) throws IllegalVariableEvaluationException {
        var renderedMap = runContext.render(pluginConfig.getCheckpointStoreProperties()).asMap(String.class, String.class);

        var connectionString = renderedMap.get("connectionString");
        var containerName = renderedMap.get("containerName");

        if (connectionString == null || connectionString.trim().isEmpty()) {
            throw new IllegalArgumentException(
                "checkpointStoreProperties.connectionString is required. Provide your Azure Blob Storage connection string."
            );
        }

        if (containerName == null || containerName.trim().isEmpty()) {
            throw new IllegalArgumentException(
                "checkpointStoreProperties.containerName is required. Provide your Azure Blob Storage container name."
            );
        }

        BlobContainerClientInterface config = BlobContainerClientInterface.builder()
            .containerName(Property.ofValue(renderedMap.get("containerName")))
            .connectionString(Property.ofValue(renderedMap.get("connectionString")))
            .sharedKeyAccountAccessKey(Property.ofValue(renderedMap.get("sharedKeyAccountAccessKey")))
            .sharedKeyAccountName(Property.ofValue(renderedMap.get("sharedKeyAccountName")))
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
            title = "Events consumed"
        )
        private final Integer eventsCount;

        @Schema(
            title = "Consumed events URI",
            description = "kestra:// URI for Ion file containing consumed events"
        )
        private URI uri;
    }
}
