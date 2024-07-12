package io.kestra.plugin.azure.storage.blob.models;

import com.azure.storage.blob.BlobContainerClient;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.AbstractConnectionInterface;
import io.kestra.plugin.azure.AzureClientInterface;
import io.kestra.plugin.azure.storage.blob.services.BlobService;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
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
public class BlobStorageForBatch implements AzureClientInterface, AbstractConnectionInterface {
    protected String endpoint;
    protected String connectionString;
    protected String sharedKeyAccountName;
    protected String sharedKeyAccountAccessKey;

    @Schema(
        title = "The URL of the blob container the compute node should use.",
        description = "Mandatory if you want to use `namespaceFiles`, `inputFiles` or `outputFiles` properties."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String containerName;

    public boolean valid() {
        return this.containerName != null &&
            (
                this.connectionString != null ||
                    (this.endpoint != null && this.sharedKeyAccountName != null && this.sharedKeyAccountAccessKey != null)
            );
    }

    public BlobContainerClient blobContainerClient(RunContext runContext) throws IllegalVariableEvaluationException {
        return BlobService.client(
            this.endpoint,
            this.connectionString,
            this.sharedKeyAccountName,
            this.sharedKeyAccountAccessKey,
            null,
            runContext
        ).getBlobContainerClient(runContext.render(containerName));
    }
}
