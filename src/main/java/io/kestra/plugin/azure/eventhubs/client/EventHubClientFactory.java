package io.kestra.plugin.azure.eventhubs.client;

import com.azure.core.amqp.AmqpRetryMode;
import com.azure.core.amqp.AmqpRetryOptions;
import com.azure.core.credential.AzureNamedKeyCredential;
import com.azure.core.credential.AzureSasCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubProducerAsyncClient;
import com.azure.messaging.eventhubs.EventProcessorClientBuilder;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.plugin.azure.client.AzureClientConfig;
import io.kestra.plugin.azure.eventhubs.config.BlobContainerClientConfig;
import io.kestra.plugin.azure.eventhubs.config.EventHubClientConfig;
import io.kestra.plugin.azure.eventhubs.config.EventHubConsumerConfig;
//import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

//@Slf4j
public class EventHubClientFactory {

    /**
     * Factory method for constructing a new {@link EventHubClientBuilder} for the given config.
     *
     * @param config The configuration. Cannot be {@code null}.
     * @return a new {@link EventHubClientBuilder} object.
     */
    public EventHubProducerAsyncClient createAsyncProducerClient(final EventHubClientConfig<?> config) throws IllegalVariableEvaluationException {
        Objects.requireNonNull(config, "config should not be null");
        return createBuilder(config).buildAsyncProducerClient();
    }

    private Optional<String> fullyQualifiedNamespace(final EventHubClientConfig<?> config) throws IllegalVariableEvaluationException {
        return config.namespace().map(ns -> ns + ".servicebus.windows.net");
    }

    /**
     * Factory method for constructing a new {@link EventProcessorClientBuilder} for the given config.
     *
     * @param config The configuration. Cannot be {@code null}.
     * @return a new {@link EventProcessorClientBuilder} object.
     */
    public EventProcessorClientBuilder createEventProcessorClientBuilder(final EventHubConsumerConfig config) throws IllegalVariableEvaluationException {
        Objects.requireNonNull(config, "config should not be null");
        EventProcessorClientBuilder builder = new EventProcessorClientBuilder()
            .eventHubName(config.eventHubName())
            .consumerGroup(config.consumerGroup())
            .retryOptions(getRetryOptions(config));

        config.customEndpointAddress().ifPresent(builder::customEndpointAddress);
        fullyQualifiedNamespace(config).ifPresent(builder::fullyQualifiedNamespace);

        Optional<String> connectionString = connectionString(config, "EventHubClient");

        if (connectionString.isPresent()) {
            return builder.connectionString(connectionString.get());
        }

        Optional<AzureNamedKeyCredential> azureNamedKeyCredential = namedKeyCredential(config, "EventHubClient");
        if (azureNamedKeyCredential.isPresent()) {
            return builder.credential(azureNamedKeyCredential.get());
        }

        Optional<AzureSasCredential> azureSasCredential = sasCredential(config, "EventHubClient");
        if (azureSasCredential.isPresent()) {
            return builder.credential(azureSasCredential.get());
        }

        return builder.credential(new DefaultAzureCredentialBuilder().build());
    }

    /**
     * Factory method for constructing a new {@link EventHubClientBuilder} for the given config.
     *
     * @param config The configuration.
     * @return a new {@link EventHubClientBuilder} object.
     */
    private EventHubClientBuilder createBuilder(EventHubClientConfig<?> config) throws IllegalVariableEvaluationException {

        EventHubClientBuilder builder = new EventHubClientBuilder()
            .eventHubName(config.eventHubName())
            .retryOptions(getRetryOptions(config));

        config.customEndpointAddress().ifPresent(builder::customEndpointAddress);
        fullyQualifiedNamespace(config).ifPresent(builder::fullyQualifiedNamespace);

        Optional<String> connectionString = connectionString(config, "EventHubClient");
        if (connectionString.isPresent()) {
            return builder.connectionString(connectionString.get());
        }

        Optional<AzureNamedKeyCredential> azureNamedKeyCredential = namedKeyCredential(config, "EventHubClient");
        if (azureNamedKeyCredential.isPresent()) {
            return builder.credential(azureNamedKeyCredential.get());
        }

        Optional<AzureSasCredential> azureSasCredential = sasCredential(config, "EventHubClient");
        if (azureSasCredential.isPresent()) {
            return builder.credential(azureSasCredential.get());
        }

        return builder.credential(new DefaultAzureCredentialBuilder().build());
    }

    private static AmqpRetryOptions getRetryOptions(EventHubClientConfig<?> config) throws IllegalVariableEvaluationException {
        return new AmqpRetryOptions()
            .setMode(AmqpRetryMode.EXPONENTIAL)
            .setDelay(config.clientRetryDelay().map(Duration::ofMillis).orElse(Duration.ofMillis(500)))
            .setMaxRetries(config.clientMaxRetries().orElse(5))
            .setMaxDelay(Duration.ofMinutes(1));
    }

    /**
     * Factory method for constructing a new {@link BlobContainerAsyncClient} for the given config.
     *
     * @param config The configuration. Cannot be {@code null}.
     * @return a new {@link BlobContainerAsyncClient} object.
     */
    public BlobContainerAsyncClient createBlobContainerAsyncClient(BlobContainerClientConfig config) throws IllegalVariableEvaluationException {
        BlobContainerClientBuilder builder = new BlobContainerClientBuilder()
            .containerName(config.containerName());

        Optional<String> connectionString = connectionString(config, "BlobContainerClient");
        if (connectionString.isPresent()) {
            return builder.connectionString(connectionString.get())
                .buildAsyncClient();
        }

        Optional<AzureNamedKeyCredential> azureNamedKeyCredential = namedKeyCredential(config, "BlobContainerClient");
        if (azureNamedKeyCredential.isPresent()) {
            return builder.credential(azureNamedKeyCredential.get())
                .buildAsyncClient();
        }

        Optional<AzureSasCredential> azureSasCredential = sasCredential(config, "BlobContainerClient");
        if (azureSasCredential.isPresent()) {
            return builder.credential(azureSasCredential.get())
                .buildAsyncClient();
        }

        return builder.credential(new DefaultAzureCredentialBuilder().build()).buildAsyncClient();
    }

    private Optional<String> connectionString(final AzureClientConfig<?> config,
                                              final String clientName) throws IllegalVariableEvaluationException {
        Optional<String> optionalConnectionString = config.connectionString();
        if (optionalConnectionString.isPresent()) {
            log.debug("Creating new {} using the `connectionString` that was passed "
                + "through the task's configuration", clientName);
        }
        return optionalConnectionString;
    }

    private Optional<AzureSasCredential> sasCredential(final AzureClientConfig<?> config,
                                                       final String clientName) throws IllegalVariableEvaluationException {
        if (config.sasToken().isPresent()) {
            log.debug("Creating new {} using the `sasToken`" +
                " that was passed through the task's configuration", clientName);
            AzureSasCredential credential = new AzureSasCredential(config.sasToken().get());
            return Optional.of(credential);
        }
        return Optional.empty();
    }

    private Optional<AzureNamedKeyCredential> namedKeyCredential(final AzureClientConfig<?> config,
                                                                 final String clientName) throws IllegalVariableEvaluationException {
        if (config.sharedKeyAccountAccessKey().isPresent() && config.sharedKeyAccountName().isPresent()) {
            log.debug("Creating new {} using the `sharedKeyAccountName` and `sharedKeyAccountAccessKey`" +
                " that was passed through the task's configuration", clientName);
            AzureNamedKeyCredential credential = new AzureNamedKeyCredential(
                config.sharedKeyAccountName().get(),
                config.sharedKeyAccountAccessKey().get()
            );
            return Optional.of(credential);
        }
        return Optional.empty();
    }
}
