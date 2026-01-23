package io.kestra.plugin.azure.servicebus;

import com.azure.messaging.servicebus.*;
import com.azure.messaging.servicebus.models.ServiceBusReceiveMode;
import com.azure.messaging.servicebus.models.SubQueue;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;

import static io.kestra.plugin.azure.servicebus.AbstractServiceBusTask.*;
import static io.kestra.plugin.azure.servicebus.Consume.RECEIVE_MODE_DESCRIPTION;
import static io.kestra.plugin.azure.servicebus.Consume.SUB_QUEUE_DESCRIPTION;


@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: service_bus_listen
                namespace: company.team

                tasks:
                  - id: log_messages
                    type: io.kestra.plugin.core.log.Log
                    message: "Trigger body: {{trigger.body}}."

                triggers:
                  - id: watch
                    type: io.kestra.plugin.azure.servicebus.RealTimeTrigger
                    queueName: your-queue-name
                    tenantId: "{{ secret('AZURE_TENANT_ID') }}"
                    clientId: "{{ secret('AZURE_CLIENT_ID') }}"
                    clientSecret: "{{ secret('AZURE_CLIENT_SECRET') }}"
                """
        )
    }
)
public class RealTimeTrigger extends AbstractTrigger implements RealtimeTriggerInterface, TriggerOutput<Message> {
    @Schema(description = QUEUE_NAME_DESCRIPTION)
    private Property<String> queueName;

    @Schema(description = TOPIC_NAME_DESCRIPTION)
    private Property<String> topicName;

    @Schema(description = CONNECTION_STRING_DESCRIPTION)
    private Property<String> connectionString;

    @Schema(description = SUBSCRIPTION_NAME_DESCRIPTION)
    private Property<String> subscriptionName;

    @Schema(description = RECEIVE_MODE_DESCRIPTION)
    private Property<ServiceBusReceiveMode> receiveMode;

    @Schema(description = SUB_QUEUE_DESCRIPTION)
    private Property<SubQueue> subQueue;

    @Builder.Default
    @NotNull
    @Schema(title = SERDE_TYPE_DESCRIPTION)
    private Property<SerdeType> serdeType = Property.ofValue(SerdeType.STRING);

    @Schema
    private Property<String> tenantId;

    @Schema
    private Property<String> clientId;

    @Schema
    private Property<String> clientSecret;

    @Schema
    private Property<String> pemCertificate;

    private static final ServiceBusReceiveMode DEFAULT_RECEIVE_MODE = ServiceBusReceiveMode.PEEK_LOCK;


    @Override
    public Flux<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        SerdeType rSerdeType = runContext.render(this.serdeType).as(SerdeType.class).orElse(SerdeType.STRING);

        Consume consume = Consume.builder()
            .topicName(topicName)
            .queueName(queueName)
            .connectionString(connectionString)
            .subscriptionName(subscriptionName)
            .clientId(clientId)
            .receiveMode(receiveMode)
            .subQueue(subQueue)
            .pemCertificate(pemCertificate)
            .clientSecret(clientSecret)
            .tenantId(tenantId)
            .build();

        var client = consume.getServiceBusReceiverClientBuilder(runContext).disableAutoComplete().buildAsyncClient();
        return client.receiveMessages().map(
            (ServiceBusReceivedMessage serviceBusMessage) -> Message.builder()
                .body(rSerdeType.deserialize(serviceBusMessage.getBody()))
                .messageId(serviceBusMessage.getMessageId())
                .subject(serviceBusMessage.getSubject())
                .build()
        ).map(
            (message) -> TriggerService.generateRealtimeExecution(
                this,
                conditionContext,
                context,
                message
            )
        ).doFinally(signalType -> client.close());

    }
}
