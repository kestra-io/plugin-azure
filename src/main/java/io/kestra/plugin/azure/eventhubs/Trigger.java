package io.kestra.plugin.azure.eventhubs;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.eventhubs.serdes.Serdes;
import io.kestra.plugin.azure.eventhubs.service.consumer.StartingPosition;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;


import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * The {@link Trigger} can be used for triggering flow based on events received from Azure Event Hubs.
 */
@Plugin(examples = {
    @Example(
        title = "Trigger flow based on events received from Azure Event Hubs in batch.",
        full = true,
        code = """
            id: azure_eventhubs_trigger
            namespace: company.team

            tasks:
              - id: log
                type: io.kestra.plugin.core.log.Log
                message: Hello there! I received {{ trigger.eventsCount }} from Azure EventHubs!

            triggers:
              - id: read_from_eventhub
                type: io.kestra.plugin.azure.eventhubs.Trigger
                interval: PT30S
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
})
@Schema(
    title = "Poll Azure Event Hubs and trigger flows",
    description = "Periodically consumes events in batches, checkpoints to Blob Storage, and triggers one execution per batch. Defaults: interval=PT60S, consumerGroup=$Default, partitionStartingPosition=EARLIEST, maxBatchSizePerPartition=50, maxWaitTimePerPartition=PT5S, maxDuration=PT10S. Use RealtimeTrigger for per-event executions."
)

@NoArgsConstructor
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
public class Trigger extends AbstractTrigger implements EventHubConsumerInterface, EventHubBatchConsumerInterface, PollingTriggerInterface, TriggerOutput<Consume.Output> {

    // TRIGGER'S PROPERTIES
    @Builder.Default
    @Schema(title = "Polling interval", description = "Time between poll cycles; defaults to PT60S")
    private Duration interval = Duration.ofSeconds(60);

    // TASK'S PARAMETERS
    protected Property<String> connectionString;

    protected Property<String> sharedKeyAccountName;

    protected Property<String> sharedKeyAccountAccessKey;

    protected Property<String> sasToken;

    @Builder.Default
    protected Property<Integer> clientMaxRetries = Property.ofValue(5);

    @Builder.Default
    protected Property<Long> clientRetryDelay = Property.ofValue(500L);

    @Builder.Default
    @Schema(title = "Body deserializer", description = "Serde used to decode event bodies; defaults to STRING")
    private Property<Serdes> bodyDeserializer = Property.ofValue(Serdes.STRING);

    @Builder.Default
    @Schema(title = "Deserializer properties", description = "Key/value options passed to the selected serde")
    private Property<Map<String, Object>> bodyDeserializerProperties = Property.ofValue(new HashMap<>());

    @Builder.Default
    @Schema(title = "Consumer group", description = "Event Hubs consumer group; defaults to $Default")
    private Property<String> consumerGroup = Property.ofValue("$Default");

    @Builder.Default
    @Schema(title = "Starting position", description = "Initial position strategy per partition; defaults to EARLIEST")
    private Property<StartingPosition> partitionStartingPosition = Property.ofValue(StartingPosition.EARLIEST);

    @Schema(title = "Start from enqueue time", description = "Optional enqueue time filter (ISO-8601); overrides starting position")
    private Property<String> enqueueTime;

    @Builder.Default
    @Schema(title = "Max batch size per partition", description = "Maximum events pulled per partition read; defaults to 50")
    private Property<Integer> maxBatchSizePerPartition = Property.ofValue(50);

    @Builder.Default
    @Schema(title = "Max wait per partition", description = "Maximum wait for a partition batch before returning; defaults to PT5S")
    private Property<Duration> maxWaitTimePerPartition = Property.ofValue(Duration.ofSeconds(5));

    @Builder.Default
    @Schema(title = "Overall max duration", description = "Stop consuming after this duration each poll; defaults to PT10S")
    private Property<Duration> maxDuration = Property.ofValue(Duration.ofSeconds(10));

    @NotNull
    @Schema(title = "Checkpoint store properties", description = "Blob container config for checkpoints (connectionString, containerName required)")
    private Property<Map<String, String>> checkpointStoreProperties;

    private Property<String> namespace;

    private Property<String> eventHubName;

    private Property<String> customEndpointAddress;

    /**
     * {@inheritDoc}
     **/
    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext,
                                        TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();

        final Consume task = new Consume();
        final Consume.Output output = task.run(runContext, this);

        if (output.getEventsCount() == 0) {
            return Optional.empty();
        }

        Execution execution = TriggerService.generateExecution(this, conditionContext, context, output);

        return Optional.of(execution);
    }
}
