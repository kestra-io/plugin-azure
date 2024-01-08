package io.kestra.plugin.azure.eventhubs.config;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.client.AzureClientConfig;
import io.kestra.plugin.azure.eventhubs.BlobContainerClientInterface;

/**
 * Configuration that uses the {@link RunContext} to render configuration.
 */
public class BlobContainerClientConfig extends AzureClientConfig<BlobContainerClientInterface> {

    /**
     * Creates a new {@link BlobContainerClientConfig} instance.
     *
     * @param runContext The context. Cannot be null.
     * @param plugin     The plugin. Cannot be null.
     */
    public BlobContainerClientConfig(final RunContext runContext,
                                     final BlobContainerClientInterface plugin) {
        super(runContext, plugin);
    }

    public String containerName() throws IllegalVariableEvaluationException {
        return runContext.render(plugin.getContainerName());
    }
}
