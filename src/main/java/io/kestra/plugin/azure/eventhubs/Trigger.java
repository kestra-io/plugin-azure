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
import lombok.*;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Collections;
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
    title = "Trigger a flow on message consumption periodically from Azure Event Hubs.",
    description = "If you would like to consume each message from Azure Event Hubs in real-time and create one execution per message, you can use the [io.kestra.plugin.azure.eventhubs.RealtimeTrigger](https://kestra.io/plugins/plugin-azure/triggers/io.kestra.plugin.azure.eventhubs.realtimetrigger) instead."
)
@Slf4j
@NoArgsConstructor
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
public class Trigger extends AbstractTrigger implements EventHubConsumerInterface, EventHubBatchConsumerInterface, PollingTriggerInterface, TriggerOutput<Consume.Output> {

    // TRIGGER'S PROPERTIES
    @Builder.Default
    private Duration interval = Duration.ofSeconds(60);

    // TASK'S PARAMETERS
    protected Property<String> connectionString;

    protected Property<String> sharedKeyAccountName;

    protected Property<String> sharedKeyAccountAccessKey;

    protected Property<String> sasToken;

    @Builder.Default
    protected Property<Integer> clientMaxRetries = Property.of(5);

    @Builder.Default
    protected Property<Long> clientRetryDelay = Property.of(500L);

    @Builder.Default
    private Property<Serdes> bodyDeserializer = Property.of(Serdes.STRING);

    @Builder.Default
    private Property<Map<String, Object>> bodyDeserializerProperties = Property.of(new HashMap<>());

    @Builder.Default
    private Property<String> consumerGroup = Property.of("$Default");

    @Builder.Default
    private Property<StartingPosition> partitionStartingPosition = Property.of(StartingPosition.EARLIEST);

    private Property<String> enqueueTime;

    @Builder.Default
    private Property<Integer> maxBatchSizePerPartition = Property.of(50);

    @Builder.Default
    private Property<Duration> maxWaitTimePerPartition = Property.of(Duration.ofSeconds(5));

    @Builder.Default
    private Property<Duration> maxDuration = Property.of(Duration.ofSeconds(10));

    @Builder.Default
    private Property<Map<String, String>> checkpointStoreProperties = Property.of(new HashMap<>());

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
