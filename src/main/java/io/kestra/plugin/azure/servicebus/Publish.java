package io.kestra.plugin.azure.servicebus;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
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
import lombok.extern.jackson.Jacksonized;

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
                id: azure_cosmos_container_create_item
                namespace: company.team

                tasks:
                  - id: create
                    type: io.kestra.plugin.azure.storage.cosmosdb.CreateItem
                    endpoint: "https://yourcosmosaccount.documents.azure.com"
                    databaseId: your_data_base_id
                    containerId: your_container_id
                    tenantId: "{{ secret('AZURE_TENANT_ID') }}"
                    clientId: "{{ secret('AZURE_CLIENT_ID') }}"
                    clientSecret: "{{ secret('AZURE_CLIENT_SECRET') }}"
                    item:
                      id: item_id
                      key: value
                """
        )
    }
)
public class Publish extends AbstractServiceBusTask implements RunnableTask<Publish.Output> {
    @NotNull
    @Schema(title = "The message to publish")
    Property<Object> from;

    @Override
    public Publish.Output run(RunContext runContext) throws Exception {
        Optional<String> rQueueName = runContext.render(this.queueName).as(String.class);
        Optional<String> rTopicName = runContext.render(this.topicName).as(String.class);
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
                    ServiceBusMessage sendMessageRequest = new ServiceBusMessage(message.getBody())
                        .setMessageId(message.getMessageId())
                        .setSubject(message.getSubject());

                    sender.sendMessage(sendMessageRequest);
                    return 1;
                })).reduce(Integer::sum).blockOptional().orElse(0);


            rQueueName.ifPresent(queueName -> runContext.metric(
                Counter.of("servicebus.publish.messages", count, "queue", queueName)
            ));
            rTopicName.ifPresent(topicName -> runContext.metric(
                Counter.of("servicebus.publish.messages", count, "topic", rTopicName.get())
            ));
            return new Output(count);
        }
    }

    @Builder
    @Getter
    @Jacksonized
    public static class Message {

        private final String messageId;
        private final String subject;
        private final String body;

        public Message(String messageId, String subject, String body) {
            this.messageId = messageId;
            this.subject = subject;
            this.body = body;
        }
    }

    public record Output(
        Integer messagesCount
    ) implements io.kestra.core.models.tasks.Output {}
}
