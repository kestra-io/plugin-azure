package io.kestra.plugin.azure.eventhubs;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventProcessorClient;
import com.azure.messaging.eventhubs.models.PartitionContext;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The {@link RealtimeTrigger} can be used for triggering flow based on events received from Azure Event Hubs.
 */
@Plugin(examples = {
    @Example(
        full = true,
        title = "Trigger flow based on events received from Azure Event Hubs in real-time.",
        code = """
            id: azure_eventhubs_realtime_trigger
            namespace: company.team

            tasks:
              - id: log
                type: io.kestra.plugin.core.log.Log
                message: Hello there! I received {{ trigger.body }} from Azure EventHubs!

            triggers:
              - id: read_from_eventhub
                type: io.kestra.plugin.azure.eventhubs.RealtimeTrigger
                eventHubName: my_eventhub
                namespace: my_eventhub_namespace
                connectionString: "{{ secret('EVENTHUBS_CONNECTION') }}"
                bodyDeserializer: JSON
                consumerGroup: "$Default"
                checkpointStoreProperties:
                  containerName: kestra
                  connectionString: "{{ secret('BLOB_CONNECTION') }}"
            """
    ),
    @Example(
        full = true,
        title = "Use Azure Eventhubs Realtime Trigger to push events into StorageTable",
        code = """
            id: eventhubs_realtime_trigger
            namespace: company.team
            
            tasks:
              - id: insert_into_storagetable
                type: io.kestra.plugin.azure.storage.table.Bulk
                endpoint: https://yourstorageaccount.blob.core.windows.net
                connectionString: "{{ secret('STORAGETABLE_CONNECTION') }}"
                table: orders
                from:
                  - partitionKey: order_id
                    rowKey: "{{ trigger.body | jq('.order_id') | first }}"
                    properties:
                      customer_name: "{{ trigger.body | jq('.customer_name') | first }}"
                      customer_email: "{{ trigger.body | jq('.customer_email') | first }}"
                      product_id: "{{ trigger.body | jq('.product_id') | first }}"
                      price: "{{ trigger.body | jq('.price') | first }}"
                      quantity: "{{ trigger.body | jq('.quantity') | first }}"
                      total: "{{ trigger.body | jq('.total') | first }}"
            
            triggers:
              - id: realtime_trigger
                type: io.kestra.plugin.azure.eventhubs.RealtimeTrigger
                eventHubName: orders
                namespace: kestra
                connectionString: "{{ secret('EVENTHUBS_CONNECTION') }}"
                bodyDeserializer: JSON
                consumerGroup: $Default
                checkpointStoreProperties:
                  containerName: kestra
                  connectionString: "{{ secret('BLOB_CONNECTION') }}"
        """
    )   
})
@Schema(
    title = "Consume a message in real-time from a Azure Event Hubs and create one execution per message.",
    description = "If you would like to consume multiple messages processed within a given time frame and process them in batch, you can use the [io.kestra.plugin.azure.eventhubs.Trigger](https://kestra.io/plugins/plugin-azure/triggers/io.kestra.plugin.azure.eventhubs.trigger) instead."
)
@Slf4j
@NoArgsConstructor
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
public class RealtimeTrigger extends AbstractTrigger implements EventHubConsumerInterface, RealtimeTriggerInterface, TriggerOutput<EventDataOutput> {

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
    private Property<Map<String, String>> checkpointStoreProperties = Property.of(new HashMap<>());

    private Property<String> namespace;

    private Property<String> eventHubName;

    private Property<String> customEndpointAddress;

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
            .map(event -> TriggerService.generateRealtimeExecution(this, conditionContext, context, event));
    }

    public Publisher<EventDataOutput> publisher(final Consume task,
                                                final RunContext runContext) throws Exception {

        final EventHubConsumerService service = task.newEventHubConsumerService(runContext, task);
        final EventDataObjectConverter converter = task.newConverter(task, runContext);

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
