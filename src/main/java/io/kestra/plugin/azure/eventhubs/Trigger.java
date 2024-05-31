package io.kestra.plugin.azure.eventhubs;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
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
import java.util.Map;
import java.util.Optional;

/**
 * The {@link Trigger} can be used for triggering flow based on events received from Azure Event Hubs.
 */
@Plugin(examples = {
    @Example(
        title = "Trigger flow based on events received from Azure Event Hubs in batch.",
        full = true,
        code = {
            """
                id: TriggerFromAzureEventHubs
                namespace: company.team
                tasks:
                  - id: hello
                    type: io.kestra.plugin.core.log.Log
                    message: Hello there! I received {{ trigger.eventsCount }} from Azure EventHubs!
                triggers:
                  - id: readFromEventHubs
                    type: "io.kestra.plugin.azure.eventhubs.Trigger"
                    interval: PT30S
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
    title = "Consume messages periodically from Azure Event Hubs and create one execution per batch.",
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
    protected String connectionString;

    protected String sharedKeyAccountName;

    protected String sharedKeyAccountAccessKey;

    protected String sasToken;

    @Builder.Default
    protected Integer clientMaxRetries = 5;

    @Builder.Default
    protected Long clientRetryDelay = 500L;

    @Builder.Default
    private Serdes bodyDeserializer = Serdes.STRING;

    @Builder.Default
    private Map<String, Object> bodyDeserializerProperties = Collections.emptyMap();

    @Builder.Default
    private String consumerGroup = "$Default";

    @Builder.Default
    private StartingPosition partitionStartingPosition = StartingPosition.EARLIEST;

    private String enqueueTime;

    @Builder.Default
    private Integer maxBatchSizePerPartition = 50;

    @Builder.Default
    private Duration maxWaitTimePerPartition = Duration.ofSeconds(5);

    @Builder.Default
    private Duration maxDuration = Duration.ofSeconds(10);

    @Builder.Default
    private Map<String, String> checkpointStoreProperties = Collections.emptyMap();

    private String namespace;

    private String eventHubName;

    private String customEndpointAddress;

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
