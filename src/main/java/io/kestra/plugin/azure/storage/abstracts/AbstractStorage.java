package io.kestra.plugin.azure.storage.abstracts;

import io.kestra.core.models.property.Property;
import io.kestra.plugin.azure.AbstractConnection;
import io.kestra.plugin.azure.AzureClientInterface;
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
    protected Property<String> connectionString;

    protected Property<String> sharedKeyAccountName;

    protected Property<String> sharedKeyAccountAccessKey;
}
