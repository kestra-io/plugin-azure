package io.kestra.plugin.azure.servicebus;

import com.azure.messaging.servicebus.administration.ServiceBusAdministrationClient;
import com.azure.messaging.servicebus.administration.ServiceBusAdministrationClientBuilder;
import com.azure.messaging.servicebus.administration.models.*;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@KestraTest(startRunner = true)
public class BaseServiceBusTest {
    @Value("${kestra.variables.globals.azure.servicebus.queue-name}")
    protected String queueName;

    @Value("${kestra.variables.globals.azure.servicebus.topic-name}")
    protected String topicName;

    @Value("${kestra.variables.globals.azure.servicebus.connection-string}")
    protected String connectionString;

    @Inject
    protected RunContextFactory runContextFactory = new RunContextFactory();

    protected final String testId = IdUtils.create();

    protected static final Map<String, Object> singleMessage = Map.of(
        "messageId", "messangeId",
        "body", "messageBodyExample"
    );

    protected static final List<Map<String, Object>> messages = List.of(singleMessage);

    protected String subscriptionName;

    private final List<String> subscriptions = new ArrayList<>();

    @BeforeAll
    void setUp () throws Exception {
        subscriptionName = this.createSubscription(topicName);
    }

    @AfterAll
    void tearDown() {
        ServiceBusAdministrationClient admin = new ServiceBusAdministrationClientBuilder()
            .connectionString(connectionString)
            .buildClient();
        subscriptions.forEach(subscriptionName -> admin.deleteSubscription(topicName, subscriptionName));
    }

    @AfterEach
    void clearUp() throws Exception {
        clearTopic();
    }

    protected void publishToTopic(Message.MessageBuilder messageBuilder) throws Exception {
        publishToTopic(List.of(messageBuilder));
    }

    protected void publishToTopic(List<Message.MessageBuilder> messageBuilders) throws Exception {
        messageBuilders.forEach(messageBuilder ->
            messageBuilder.applicationProperties(Map.of(
                    "targetSubscription", testId
            ))
        );

        List<Message> messages = messageBuilders.stream()
            .map((messageBuilder) -> messageBuilder.applicationProperties(Map.of(
                "targetSubscription", testId
            )))
            .map(Message.MessageBuilder::build)
            .toList();

        Publish.builder()
            .from(Property.ofValue(messages))
            .topicName(Property.ofValue(topicName))
            .connectionString(Property.ofValue(connectionString))
            .build()
            .run(runContextFactory.of());
    }

    protected String createSubscription(String subscriptionNamePrefix) {
        String subscriptionName = subscriptionNamePrefix + "-" + testId;
        ServiceBusAdministrationClient admin = new ServiceBusAdministrationClientBuilder()
            .connectionString(connectionString)
            .buildClient();

        admin.createSubscription(
            topicName,
            subscriptionName,
            "test-subscription-rule",
            new CreateSubscriptionOptions(),
            new CreateRuleOptions().setFilter(
                new SqlRuleFilter(
                    String.format("targetSubscription = '%s'", testId))
            )
        );
        subscriptions.add(subscriptionName);
        return subscriptionName;
    }

    private void clearTopic() throws Exception {
        Consume.builder()
            .topicName(Property.ofValue(topicName))
            .subscriptionName(Property.ofValue(subscriptionName))
            .connectionString(Property.ofValue(connectionString))
            .maxReceiveDuration(Property.ofValue(Duration.ofNanos(500)))
            .build().run(runContextFactory.of());
    }
}
