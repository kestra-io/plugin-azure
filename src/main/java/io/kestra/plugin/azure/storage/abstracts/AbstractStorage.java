package io.kestra.plugin.azure.storage.abstracts;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.plugin.azure.shared.AbstractConnection;
import io.kestra.plugin.azure.shared.AzureClientInterface;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractStorage extends AbstractConnection implements AzureClientInterface {
    @PluginProperty(secret = true)
    protected Property<String> connectionString;

    protected Property<String> sharedKeyAccountName;

    @PluginProperty(secret = true)
    protected Property<String> sharedKeyAccountAccessKey;
}
