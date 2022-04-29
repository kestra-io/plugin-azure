package io.kestra.plugin.azure.storage.table.abstracts;

import com.azure.core.credential.AzureNamedKeyCredential;
import com.azure.data.tables.TableClient;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.TableServiceClientBuilder;
import com.azure.identity.DefaultAzureCredentialBuilder;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.storage.abstracts.AbstractStorage;
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
public abstract class AbstractTableStorage extends AbstractStorage implements AbstractTableStorageInterface {
    protected String table;

    protected TableServiceClient client(RunContext runContext) throws IllegalVariableEvaluationException {
        TableServiceClientBuilder builder = new TableServiceClientBuilder()
            .endpoint(runContext.render(endpoint));

        if (this.connectionString != null) {
            builder.connectionString(runContext.render(connectionString));
        } else if (this.sharedKeyAccountName != null && this.sharedKeyAccountAccessKey != null) {
            builder.credential(new AzureNamedKeyCredential(
                runContext.render(this.sharedKeyAccountName),
                runContext.render(this.sharedKeyAccountAccessKey)
            ));
        } else if (this.sasToken != null ) {
            builder.sasToken(runContext.render(this.sasToken));
        } else {
            builder.credential(new DefaultAzureCredentialBuilder().build());
        }


        return builder.buildClient();
    }

    protected TableClient tableClient(RunContext runContext) throws IllegalVariableEvaluationException {
        TableServiceClient client = this.client(runContext);

        return client.getTableClient(runContext.render(this.table));
    }
}
