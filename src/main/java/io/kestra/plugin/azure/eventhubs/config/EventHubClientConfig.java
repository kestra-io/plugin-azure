package io.kestra.plugin.azure.eventhubs.config;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.client.AzureClientConfig;
import io.kestra.plugin.azure.eventhubs.EventHubClientInterface;

import java.util.Optional;

/**
 * Configuration that uses the {@link RunContext} to render configuration.
 */
public class EventHubClientConfig<T extends EventHubClientInterface> extends AzureClientConfig<T> {

    /**
     * Creates a new {@link EventHubClientConfig} instance.
     *
     * @param runContext The context. Cannot be null.
     * @param plugin     The plugin. Cannot be null.
     */
    public EventHubClientConfig(final RunContext runContext,
                                final T plugin) {
        super(runContext, plugin);
    }

    public String eventHubName() throws IllegalVariableEvaluationException {
        return runContext.render(plugin.getEventHubName()).as(String.class).orElse(null);
    }

    public Optional<Integer> clientMaxRetries() throws IllegalVariableEvaluationException {
        return runContext.render(plugin.getClientMaxRetries()).as(Integer.class);
    }

    public Optional<Long> clientRetryDelay() throws IllegalVariableEvaluationException {
        return runContext.render(plugin.getClientRetryDelay()).as(Long.class);
    }

    public Optional<String> namespace() throws IllegalVariableEvaluationException {
        return getOptionalConfig(plugin::getNamespace);
    }

    public Optional<String> customEndpointAddress() throws IllegalVariableEvaluationException {
        return getOptionalConfig(plugin::getCustomEndpointAddress);
    }
}
