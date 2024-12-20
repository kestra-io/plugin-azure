package io.kestra.plugin.azure.storage.blob.models;

import com.azure.storage.blob.BlobContainerClient;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
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
    protected Property<String> endpoint;
    protected Property<String> connectionString;
    protected Property<String> sharedKeyAccountName;
    protected Property<String> sharedKeyAccountAccessKey;

    @Schema(
        title = "The URL of the blob container the compute node should use.",
        description = "Mandatory if you want to use `namespaceFiles`, `inputFiles` or `outputFiles` properties."
    )
    @NotNull
    private Property<String> containerName;

    public boolean valid() {
        return this.containerName != null &&
            (
                this.connectionString != null ||
                    (this.endpoint != null && this.sharedKeyAccountName != null && this.sharedKeyAccountAccessKey != null)
            );
    }

    public BlobContainerClient blobContainerClient(RunContext runContext) throws IllegalVariableEvaluationException {
        return BlobService.client(
            runContext.render(this.endpoint).as(String.class).orElse(null),
            runContext.render(this.connectionString).as(String.class).orElse(null),
            runContext.render(this.sharedKeyAccountName).as(String.class).orElse(null),
            runContext.render(this.sharedKeyAccountAccessKey).as(String.class).orElse(null),
            null
        ).getBlobContainerClient(runContext.render(containerName).as(String.class).orElseThrow());
    }
}
