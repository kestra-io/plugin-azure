package io.kestra.plugin.azure.storage.blob.abstracts;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
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
public abstract class AbstractBlobStorageObject extends AbstractBlobStorage implements AbstractBlobStorageObjectInterface, AbstractBlobStorageContainerInterface {
    protected String container;

    protected String name;

    protected BlobClient blobClient(RunContext runContext) throws IllegalVariableEvaluationException {
        BlobServiceClient client = this.client(runContext);
        BlobContainerClient containerClient = client.getBlobContainerClient(runContext.render(this.container));

        return containerClient.getBlobClient(runContext.render(this.name));
    }
}
