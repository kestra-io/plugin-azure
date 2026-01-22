package io.kestra.plugin.azure.servicebus;

import com.azure.messaging.servicebus.administration.ServiceBusAdministrationClient;
import com.azure.messaging.servicebus.administration.ServiceBusAdministrationClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.IdUtils;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;

import java.util.List;

@KestraTest(startRunner = true, environments = "sp")
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


    protected void publishToTopic(List<Publish.Message> messages) throws Exception {
        Publish.builder()
            .from(Property.ofValue(messages))
            .topicName(Property.ofValue(topicName))
            .connectionString(Property.ofValue(connectionString))
            .build()
            .run(runContextFactory.of());
    }

    protected String createSubscription(String subscriptionNamePrefix) throws Exception {
        String subscriptionName = subscriptionNamePrefix + "-" + testId;
        ServiceBusAdministrationClient admin = new ServiceBusAdministrationClientBuilder()
            .connectionString(connectionString)
            .buildClient();

        admin.createSubscription(topicName, subscriptionName);
        return subscriptionName;
    }
}
