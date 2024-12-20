package io.kestra.plugin.azure.storage.blob.abstracts;

import com.azure.storage.blob.BlobServiceClient;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.storage.abstracts.AbstractStorageWithSas;
import io.kestra.plugin.azure.storage.blob.services.BlobService;
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
public abstract class AbstractBlobStorageWithSas extends AbstractStorageWithSas {
    public BlobServiceClient client(RunContext runContext) throws IllegalVariableEvaluationException {
        return BlobService.client(
            runContext.render(this.endpoint).as(String.class).orElse(null),
            runContext.render(this.connectionString).as(String.class).orElse(null),
            runContext.render(this.sharedKeyAccountName).as(String.class).orElse(null),
            runContext.render(this.sharedKeyAccountAccessKey).as(String.class).orElse(null),
            runContext.render(this.sasToken).as(String.class).orElse(null)
        );
    }
}
