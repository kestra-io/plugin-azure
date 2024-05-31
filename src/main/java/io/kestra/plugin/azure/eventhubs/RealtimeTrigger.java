package io.kestra.plugin.azure.eventhubs;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventProcessorClient;
import com.azure.messaging.eventhubs.models.PartitionContext;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.eventhubs.model.EventDataObject;
import io.kestra.plugin.azure.eventhubs.model.EventDataOutput;
import io.kestra.plugin.azure.eventhubs.serdes.Serdes;
import io.kestra.plugin.azure.eventhubs.service.EventDataObjectConverter;
import io.kestra.plugin.azure.eventhubs.service.consumer.EventHubConsumerService;
import io.kestra.plugin.azure.eventhubs.service.consumer.StartingPosition;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

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
                    type: io.kestra.plugin.core.log.Log
                    message: Hello there! I received {{ trigger.body }} from Azure EventHubs!
                triggers:
                  - id: readFromEventHubs
                    type: "io.kestra.plugin.azure.eventhubs.RealtimeTrigger"
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
    title = "Consume a message in real-time from a Azure Event Hubs and create one execution per message."
)
@Slf4j
@NoArgsConstructor
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
public class RealtimeTrigger extends AbstractTrigger implements EventHubConsumerInterface, RealtimeTriggerInterface, TriggerOutput<EventDataOutput> {

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
    private Map<String, String> checkpointStoreProperties = Collections.emptyMap();

    private String namespace;

    private String eventHubName;

    private String customEndpointAddress;

    @Builder.Default
    @Getter(AccessLevel.NONE)
    private final AtomicBoolean isActive = new AtomicBoolean(true);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    private final CountDownLatch waitForTermination = new CountDownLatch(1);

    /**
     * {@inheritDoc}
     **/
    @Override
    public Publisher<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        final Consume task = Consume
            .builder()
            .connectionString(connectionString)
            .sharedKeyAccountName(sharedKeyAccountName)
            .sharedKeyAccountAccessKey(sharedKeyAccountAccessKey)
            .sasToken(sasToken)
            .clientMaxRetries(clientMaxRetries)
            .clientRetryDelay(clientRetryDelay)
            .bodyDeserializer(bodyDeserializer)
            .bodyDeserializerProperties(bodyDeserializerProperties)
            .consumerGroup(consumerGroup)
            .partitionStartingPosition(partitionStartingPosition)
            .checkpointStoreProperties(checkpointStoreProperties)
            .enqueueTime(enqueueTime)
            .namespace(namespace)
            .eventHubName(eventHubName)
            .customEndpointAddress(customEndpointAddress)
            .build();
        return Flux
            .from(publisher(task, conditionContext.getRunContext()))
            .map(event -> TriggerService.generateRealtimeExecution(this, context, event));
    }

    public Publisher<EventDataOutput> publisher(final Consume task,
                                                final RunContext runContext) throws Exception {

        final EventHubConsumerService service = task.newEventHubConsumerService(runContext, task);
        final EventDataObjectConverter converter = task.newConverter(task);

        return Flux.create(emitter -> {
                Logger contextLogger = runContext.logger();
                try {
                    EventProcessorClient client = service.createEventProcessorClientBuilder(contextLogger)
                        .processEvent(eventContext -> {
                            if (!isActive.get()) {
                                return; // return immediately if the trigger is not active (checkpoint will not be updated)
                            }

                            final EventData eventData = eventContext.getEventData();
                            if (eventData == null) return;

                            final EventDataObject dataObject = converter.convertFromEventData(eventData);

                            PartitionContext partitionContext = eventContext.getPartitionContext();
                            if (contextLogger.isTraceEnabled()) {
                                contextLogger.trace(
                                    "Received new event from eventHub {} and partitionId={} [offset={}, sequenceId={}]",
                                    partitionContext.getEventHubName(),
                                    partitionContext.getPartitionId(),
                                    dataObject.offset(),
                                    dataObject.sequenceNumber()
                                );
                            }
                            emitter.next(EventDataOutput.of(dataObject));
                            eventContext.updateCheckpoint();

                        }, Duration.ofMillis(500))
                        .processError(context -> {
                            PartitionContext partitionContext = context.getPartitionContext();
                            contextLogger.error("Failed to process eventHub: {}, partitionId: {} with consumerGroup: {}",
                                partitionContext.getEventHubName(),
                                partitionContext.getPartitionId(),
                                partitionContext.getConsumerGroup(),
                                context.getThrowable()
                            );
                            emitter.error(context.getThrowable());
                        })
                        .buildEventProcessorClient();

                    // handle dispose - invoked after complete/error.
                    emitter.onDispose(() -> {
                        try {
                            client.stop(); // cannot be invoked from EventProcessorClient thread.
                        } finally {
                            waitForTermination.countDown();
                        }
                    });
                    client.start();
                    busyWait();
                    emitter.complete();
                } catch (Exception throwable) {
                    emitter.error(throwable);
                }
            });
    }

    private void busyWait() {
        while (isActive.get()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                isActive.set(false); // proactively stop consuming
            }
        }
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public void kill() {
        stop(true);
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public void stop() {
        stop(false); // must be non-blocking
    }

    private void stop(boolean wait) {
        if (!isActive.compareAndSet(true, false)) {
            return;
        }

        if (wait) {
            try {
                waitForTermination.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
