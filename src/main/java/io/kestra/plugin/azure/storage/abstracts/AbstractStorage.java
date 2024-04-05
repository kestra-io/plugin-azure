package io.kestra.plugin.azure.storage.abstracts;

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

    protected String connectionString;

    protected String sharedKeyAccountName;

    protected String sharedKeyAccountAccessKey;

    protected String sasToken;

    public boolean valid() {
        return super.valid() &&
            this.connectionString != null ||
            (this.sharedKeyAccountName != null && this.sharedKeyAccountAccessKey != null) ||
            this.sasToken != null;
    }
}
