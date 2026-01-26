package io.kestra.plugin.azure.servicebus;

import com.azure.core.util.BinaryData;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Data;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.List;
import java.util.Optional;

import static io.kestra.core.utils.Rethrow.throwFunction;

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
                id: azure_cosmos_service_bus_publish
                namespace: company.team

                tasks:
                  - id: create
                    type: io.kestra.plugin.azure.servicebus.Publish
                    queueName: your-queue-name
                    tenantId: "{{ secret('AZURE_TENANT_ID') }}"
                    clientId: "{{ secret('AZURE_CLIENT_ID') }}"
                    clientSecret: "{{ secret('AZURE_CLIENT_SECRET') }}"
                    from:
                      timeToLive: PT10S
                      body: "messageBody"
                """
        )
    },
    metrics = {
        @Metric(
            name = "servicebus.publish.queue.messages",
            type = Counter.TYPE,
            unit = "messages",
            description = "Number of messages published to the Service Bus queue."
        ),
        @Metric(
            name = "servicebus.publish.topic.messages",
            type = Counter.TYPE,
            unit = "messages",
            description = "Number of messages published to the Service Bus topic."
        )
    }
)
public class Publish extends AbstractServiceBusTask implements RunnableTask<Publish.Output> {
    @NotNull
    @Schema(
        title = Data.From.TITLE,
        description = Data.From.DESCRIPTION,
        anyOf = {String.class, List.class, Message.class}
    )
    Property<Object> from;

    @Override
    public Publish.Output run(RunContext runContext) throws Exception {
        Optional<String> rQueueName = runContext.render(this.queueName).as(String.class);
        Optional<String> rTopicName = runContext.render(this.topicName).as(String.class);
        SerdeType rSerdeType = runContext.render(this.serdeType).as(SerdeType.class).orElse(DEFAULT_SERDE_TYPE);

        Object rFrom = runContext.render(this.from).as(Object.class).orElseThrow(
            () -> new IllegalVariableEvaluationException("Message cannot be null or empty")
        );


        if (rQueueName.isPresent() == rTopicName.isPresent()) {
            throw new IllegalVariableEvaluationException("Exactly one of queueName or topicName must be specified.");
        }

        final ServiceBusClientBuilder.ServiceBusSenderClientBuilder senderClientBuilder = applyAuth(runContext, new ServiceBusClientBuilder())
            .sender();

        rTopicName.ifPresent(senderClientBuilder::topicName);
        rQueueName.ifPresent(senderClientBuilder::queueName);

        try (ServiceBusSenderClient sender = senderClientBuilder.buildClient()) {
            int count = Data.from(rFrom)
                .readAs(runContext, Message.class, msg -> JacksonMapper.toMap(msg, Message.class))
                .map(throwFunction(message -> {
                    BinaryData binaryData = rSerdeType.serialize(
                        message.getBody()
                    );

                    ServiceBusMessage sendMessageRequest = new ServiceBusMessage(binaryData)
                        .setMessageId(message.getMessageId())
                        .setSubject(message.getSubject());

                    if (message.getTimeToLive() != null) {
                        sendMessageRequest.setTimeToLive(message.getTimeToLive());
                    }

                    if (message.getApplicationProperties() != null && !message.getApplicationProperties().isEmpty()) {
                        sendMessageRequest.getApplicationProperties().putAll(
                            message.getApplicationProperties()
                        );
                    }

                    sender.sendMessage(sendMessageRequest);
                    return 1;
                })).reduce(Integer::sum).blockOptional().orElse(0);


            rQueueName.ifPresent(queueName -> runContext.metric(
                Counter.of("servicebus.publish.queue.messages", count, "queue", queueName)
            ));
            rTopicName.ifPresent(topicName -> runContext.metric(
                Counter.of("servicebus.publish.topic.messages", count, "topic", rTopicName.get())
            ));
            return new Output(count);
        }
    }

    public record Output(
        Integer messagesCount
    ) implements io.kestra.core.models.tasks.Output {}
}
