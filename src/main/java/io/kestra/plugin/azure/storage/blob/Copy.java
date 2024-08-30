package io.kestra.plugin.azure.storage.blob;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.storage.blob.abstracts.AbstractBlobStorageWithSas;
import io.kestra.plugin.azure.storage.blob.models.Blob;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.time.OffsetDateTime;
import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: azure_storage_blob_copy
                namespace: company.name

                tasks:
                  - id: copy
                    type: io.kestra.plugin.azure.storage.blob.Copy
                    from:
                      container: "my-bucket"
                      key: "path/to/file"
                    to:
                      container: "my-bucket2"
                      key: "path/to/file2"
                """
        )
    }
)
@Schema(
    title = "Copy a file between Azure Blob Storage."
)
public class Copy extends AbstractBlobStorageWithSas implements RunnableTask<Copy.Output> {
    @Schema(
        title = "The source from where the file should be copied."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private CopyObject from;

    @Schema(
        title = "The destination to copy the file to."
    )
    @PluginProperty(dynamic = false)
    @NotNull
    private CopyObject to;

    @Schema(
        title = "Whether to delete the source file after copy."
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    private Boolean delete = false;
    @Override
    public Output run(RunContext runContext) throws Exception {
        BlobServiceClient client = this.client(runContext);

        BlobContainerClient fromContainerClient = client.getBlobContainerClient(runContext.render(this.from.container));
        BlobClient fromBlobClient = fromContainerClient.getBlobClient(runContext.render(this.from.name));

        BlobContainerClient toContainerClient = client.getBlobContainerClient(runContext.render(this.to.container));
        BlobClient toBlobClient = toContainerClient.getBlobClient(runContext.render(this.to.name));

        OffsetDateTime expiryTime = OffsetDateTime.now().plusMinutes(15);
        BlobSasPermission permission = new BlobSasPermission().setReadPermission(true);

        BlobServiceSasSignatureValues values = new BlobServiceSasSignatureValues(expiryTime, permission)
            .setStartTime(OffsetDateTime.now());

        toBlobClient.copyFromUrl(fromBlobClient.getBlobUrl() + "?" + fromBlobClient.generateSas(values));

        if (this.delete) {
            Delete.builder()
                .id(this.id)
                .type(Delete.class.getName())
                .endpoint(this.endpoint)
                .connectionString(this.connectionString)
                .sharedKeyAccountName(this.sharedKeyAccountName)
                .sharedKeyAccountAccessKey(this.sharedKeyAccountAccessKey)
                .sasToken(this.sasToken)
                .container(this.from.container)
                .name(this.from.name)
                .build()
                .run(runContext);
        }

        return Output
            .builder()
            .blob(Blob.of(toBlobClient))

            .build();
    }

    @SuperBuilder(toBuilder = true)
    @Getter
    @NoArgsConstructor
    public static class CopyObject {
        @Schema(
            title = "The blob container."
        )
        @PluginProperty(dynamic = true)
        @NotNull
        String container;

        @Schema(
            title = "The full blob path on the container."
        )
        @PluginProperty(dynamic = true)
        @NotNull
        String name;
    }
    @SuperBuilder
    @Getter
    @NoArgsConstructor
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The copied blob."
        )
        private Blob blob;
    }
}
