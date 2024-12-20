package io.kestra.plugin.azure.storage.abstracts;

import io.kestra.core.models.property.Property;
import io.kestra.plugin.azure.AzureClientWithSasInterface;
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
public abstract class AbstractStorageWithSas extends AbstractStorage implements AzureClientWithSasInterface {
    protected Property<String> sasToken;
}