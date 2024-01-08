package io.kestra.plugin.azure.storage.blob.abstracts;

import com.azure.core.credential.AzureNamedKeyCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.storage.abstracts.AbstractStorage;
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
public abstract class AbstractBlobStorage extends AbstractStorage implements AzureClientInterface {
    protected BlobServiceClient client(RunContext runContext) throws IllegalVariableEvaluationException {
        BlobServiceClientBuilder builder = new BlobServiceClientBuilder()
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
}
