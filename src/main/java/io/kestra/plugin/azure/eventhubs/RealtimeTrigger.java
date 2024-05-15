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
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * The {@link RealtimeTrigger} can be used for triggering flow based on events received from Azure Event Hubs.
 */
@Plugin(examples = {
    @Example(
        title = "Trigger flow based on events received from Azure Event Hubs.",
        full = true,
        code = {
            """
                id: TriggerFromAzureEventHubs
                namespace: company.team
                tasks:
                  - id: hello
                    type: io.kestra.core.tasks.log.Log
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
},
    beta = true
)
@Schema(
    title = "Trigger flow based on events received from Azure Event Hubs."
)
@Slf4j
@NoArgsConstructor
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
public class RealtimeTrigger extends AbstractTrigger implements EventHubConsumerInterface, RealtimeTriggerInterface, TriggerOutput<Consume.Output> {

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
    public Publisher<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        final Consume task = Consume.builder().build();

        return Flux.from(task.stream(conditionContext.getRunContext(), this))
            .map(event -> TriggerService.generateRealtimeExecution(this, context, event));
    }
}
