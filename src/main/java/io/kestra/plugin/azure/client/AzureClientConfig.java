package io.kestra.plugin.azure.client;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.AzureClientWithSasInterface;

import java.util.Optional;
import java.util.function.Supplier;

import static io.kestra.core.utils.Rethrow.throwFunction;

/**
 * Configuration for creating a new Azure Client.
 */
public class AzureClientConfig<T extends AzureClientWithSasInterface> {

    protected final RunContext runContext;
    protected final T plugin;

    /**
     * Creates a new {@link AzureClientConfig} instance.
     *
     * @param runContext The context.
     * @param plugin     The plugin.
     */
    public AzureClientConfig(final RunContext runContext,
                             final T plugin) {
        this.runContext = runContext;
        this.plugin = plugin;
    }

    public Optional<String> connectionString() throws IllegalVariableEvaluationException {
        return getOptionalConfig(plugin::getConnectionString);
    }

    public Optional<String> sharedKeyAccountName() throws IllegalVariableEvaluationException {
        return getOptionalConfig(plugin::getSharedKeyAccountName);
    }

    public Optional<String> sharedKeyAccountAccessKey() throws IllegalVariableEvaluationException {
        return getOptionalConfig(plugin::getSharedKeyAccountAccessKey);
    }

    public Optional<String> sasToken() throws IllegalVariableEvaluationException {
        return getOptionalConfig(plugin::getSasToken);
    }

    protected Optional<String> getOptionalConfig(final Supplier<Property<String>> supplier) throws IllegalVariableEvaluationException {
        return runContext.render(supplier.get()).as(String.class);
    }
}
