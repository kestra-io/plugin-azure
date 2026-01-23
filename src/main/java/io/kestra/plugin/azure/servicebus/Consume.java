package io.kestra.plugin.azure.servicebus;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceiverClient;
import com.azure.messaging.servicebus.models.ServiceBusReceiveMode;
import com.azure.messaging.servicebus.models.SubQueue;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.kestra.core.utils.Rethrow.throwConsumer;

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
                id: azure_service_bus_consume_example
                namespace: company.team

                tasks:
                  - id: create
                    type: io.kestra.plugin.azure.servicebus.Consume
                    maxMessages: 100
                    maxReceiveDuration: PT30S
                    queueName: your-queue-name
                    tenantId: "{{ secret('AZURE_TENANT_ID') }}"
                    clientId: "{{ secret('AZURE_CLIENT_ID') }}"
                    clientSecret: "{{ secret('AZURE_CLIENT_SECRET') }}"
                """
        )
    }
)
public class Consume extends AbstractServiceBusTask implements RunnableTask<Consume.Output> {

    @Schema(description = RECEIVE_MODE_DESCRIPTION)
    protected Property<ServiceBusReceiveMode> receiveMode;

    @Schema(description = SUB_QUEUE_DESCRIPTION)
    protected Property<SubQueue> subQueue;

    @Schema(description = MAX_MESSAGES_DESCRIPTION)
    protected Property<Integer> maxMessages;

    @NotNull
    @Builder.Default
    @Schema(description = MAX_RECEIVE_DURATION_DESCRIPTION)
    protected Property<Duration> maxReceiveDuration = Property.ofValue(DEFAULT_MAX_RECEIVE_DURATION);

    @Builder.Default
    @Schema(hidden = true)
    protected Property<Integer> pullBatchSize = Property.ofValue(DEFAULT_PULL_BATCH_SIZE);

    private static final Duration DEFAULT_MAX_RECEIVE_DURATION = Duration.ofSeconds(10);
    private static final ServiceBusReceiveMode DEFAULT_RECEIVE_MODE = ServiceBusReceiveMode.PEEK_LOCK;
    private static final int DEFAULT_PULL_BATCH_SIZE = 100;

    public static final String RECEIVE_MODE_DESCRIPTION = "The receive mode for the receiver.";
    public static final String SUB_QUEUE_DESCRIPTION = "The type of the SubQueue to connect to";
    public static final String MAX_MESSAGES_DESCRIPTION ="The maximum amount of messages this tax will consume.";
    public static final String MAX_RECEIVE_DURATION_DESCRIPTION= """
        The maximum amount of time this task will wait before \
        returning all consumed messages
        """;

    @Override
    public Output run(RunContext runContext) throws Exception {
        SerdeType rSerdeType = runContext.render(this.serdeType).as(SerdeType.class).orElse(SerdeType.STRING);
        Duration rMaxReceiveDuration = runContext.render(this.maxReceiveDuration).as(Duration.class)
            .orElse(DEFAULT_MAX_RECEIVE_DURATION);
        Optional<Integer> rMaxMessages = runContext.render(this.maxMessages).as(Integer.class);

        int rDefaultPullBatchSize = runContext.render(pullBatchSize).as(Integer.class)
            .orElse(DEFAULT_PULL_BATCH_SIZE);

        Instant start = Instant.now();
        try (ServiceBusReceiverClient client = getServiceBusReceiverClientBuilder(runContext).buildClient()) {
            List<ServiceBusReceivedMessage> messages = new ArrayList<>();

            boolean endded;
            do {
                int numberOfMessagesToPull = rDefaultPullBatchSize;
                if (rMaxMessages.isPresent()) {
                    numberOfMessagesToPull = rMaxMessages.get() - messages.size();
                }

                List<ServiceBusReceivedMessage> latestReceivedMessages = client.receiveMessages(numberOfMessagesToPull, rMaxReceiveDuration).stream().toList();
                messages.addAll(latestReceivedMessages);
                endded = ended(messages.size(), start, latestReceivedMessages.size(), runContext);
            } while (!endded);

            return new Output(
                messages.size(),
                writeToFile(messages, runContext, rSerdeType)
            );
        }
    }

    public ServiceBusClientBuilder.ServiceBusReceiverClientBuilder getServiceBusReceiverClientBuilder(RunContext runContext) throws IllegalVariableEvaluationException {
        Optional<String> rQueueName = runContext.render(this.queueName).as(String.class);
        Optional<String> rTopicName = runContext.render(this.topicName).as(String.class);
        Optional<String> rSubscriptionName = runContext.render(this.subscriptionName).as(String.class);
        ServiceBusReceiveMode rReceiveMode = runContext.render(this.receiveMode).as(ServiceBusReceiveMode.class)
            .orElse(DEFAULT_RECEIVE_MODE);
        Optional<SubQueue> rSubQueue = runContext.render(this.subQueue).as(SubQueue.class);



        ServiceBusClientBuilder.ServiceBusReceiverClientBuilder receiverBuilder = applyAuth(
            runContext,
            new ServiceBusClientBuilder()
        ).receiver()
            .disableAutoComplete()
            .receiveMode(rReceiveMode);

        if (rQueueName.isPresent() == rTopicName.isPresent()) {
            throw new IllegalVariableEvaluationException("Exactly one of queueName or topicName must be specified.");
        }

        if (rTopicName.isPresent() && rSubscriptionName.isEmpty()) {
            throw new IllegalVariableEvaluationException("topicName and subscriptionName must be set in conjunction.");
        }

        rTopicName.ifPresent(receiverBuilder::topicName);
        rQueueName.ifPresent(receiverBuilder::queueName);
        rSubQueue.ifPresent(receiverBuilder::subQueue);
        rSubscriptionName.ifPresent(receiverBuilder::subscriptionName);

        return receiverBuilder;
    }

    private boolean ended(int totalMessages, Instant start, int messagesConsumed, RunContext runContext)
        throws IllegalVariableEvaluationException {
        return endedByDuration(start, runContext) || endedByMaxMessages(totalMessages, runContext) || messagesConsumed == 0;
    }

    private boolean endedByDuration(Instant start, RunContext runContext) throws IllegalVariableEvaluationException {
        Optional<Duration> rMaxDuration = runContext.render(this.maxReceiveDuration).as(Duration.class);
        return rMaxDuration.isPresent()
            && ZonedDateTime.now().toEpochSecond() > start.plus(rMaxDuration.get()).getEpochSecond();
    }

    private boolean endedByMaxMessages(int maxMessages, RunContext runContext) throws IllegalVariableEvaluationException {
        Optional<Integer> rMaxMessages = runContext.render(this.maxMessages).as(Integer.class);
        return rMaxMessages.isPresent() && rMaxMessages.get() <= maxMessages;
    }

    private URI writeToFile(List<ServiceBusReceivedMessage> messages, RunContext runContext, SerdeType serdeType)
        throws IOException {
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();

        try (var outputFile = new BufferedOutputStream(new FileOutputStream(tempFile))) {
            serdeType.deserialize(
                messages.stream().map(ServiceBusReceivedMessage::getBody).toList()
            ).forEach((throwConsumer(
                msg -> FileSerde.write(outputFile, msg)
            )));
            outputFile.flush();
            return runContext.storage().putFile(tempFile);
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Number of consumed rows."
        )
        private final Integer count;
        @Schema(
            title = "File URI containing consumed messages."
        )
        private final URI uri;
    }
}
