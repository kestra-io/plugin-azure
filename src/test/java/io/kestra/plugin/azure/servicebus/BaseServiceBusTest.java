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
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

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

    protected static final Map<String, Object> singleMessage = Map.of(
        "messageId", "messangeId",
        "body", "messageBodyExample"
    );

    protected static final List<Map<String, Object>> messages = List.of(singleMessage);

    private final List<String> subscriptions = new ArrayList<>();

    @AfterAll
    void tearDown() {
        ServiceBusAdministrationClient admin = new ServiceBusAdministrationClientBuilder()
            .connectionString(connectionString)
            .buildClient();
        subscriptions.forEach(subscriptionName -> admin.deleteSubscription(topicName, subscriptionName));
    }

    protected String publishToTopic(Message.MessageBuilder messageBuilder) throws Exception {
        return publishToTopic(List.of(messageBuilder), this.createSubscription(topicName, IdUtils.create()));
    }

    protected String publishToTopic(Message.MessageBuilder messageBuilder, String subscriptionName) throws Exception {
        return publishToTopic(List.of(messageBuilder), subscriptionName);
    }

    protected String publishToTopic(List<Message.MessageBuilder> messageBuilders, String subscriptionName) throws Exception {
        List<Message> messages = messageBuilders.stream()
            .map((messageBuilder) -> messageBuilder.applicationProperties(Map.of(
                "targetSubscription", subscriptionName
            )))
            .map(Message.MessageBuilder::build)
            .toList();

        Publish.builder()
            .from(Property.ofValue(messages))
            .topicName(Property.ofValue(topicName))
            .connectionString(Property.ofValue(connectionString))
            .build()
            .run(runContextFactory.of());

        return subscriptionName;
    }

    protected String createSubscription(String subscriptionNamePrefix, String testId) {
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
                    String.format("targetSubscription = '%s'", subscriptionName))
            )
        );
        subscriptions.add(subscriptionName);

        return subscriptionName;
    }
}
