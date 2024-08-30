package io.kestra.plugin.azure.storage.blob;

import com.azure.storage.blob.BlobClient;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.storage.blob.abstracts.AbstractBlobStorageWithSasObject;
import io.kestra.plugin.azure.storage.blob.models.AccessTier;
import io.kestra.plugin.azure.storage.blob.models.Blob;
import io.kestra.plugin.azure.storage.blob.models.BlobImmutabilityPolicy;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.net.URI;
import java.util.AbstractMap;
import java.util.Map;
import java.util.stream.Collectors;

import static io.kestra.core.utils.Rethrow.throwFunction;

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
                id: azure_storage_blob_upload
                namespace: company.name

                inputs:
                  - id: file
                    type: FILE
                
                tasks:
                  - id: upload
                    type: io.kestra.plugin.azure.storage.blob.Upload
                    endpoint: "https://yourblob.blob.core.windows.net"
                    connectionString: "DefaultEndpointsProtocol=...=="
                    container: "mydata"
                    from: "{{ inputs.myfile }}"
                    name: "myblob"
                """
        )
    }
)
@Schema(
    title = "Upload a file to the Azure Blob Storage."
)
public class Upload extends AbstractBlobStorageWithSasObject implements RunnableTask<Upload.Output> {
    @Schema(
        title = "The file from the internal storage to upload to the Azure Blob Storage."
    )
    @PluginProperty(dynamic = true)
    private String from;

    @Schema(
        title = "Metadata for the blob."
    )
    @PluginProperty(dynamic = true)
    private Map<String, String> metadata;

    @Schema(
        title = "User defined tags."
    )
    @PluginProperty(dynamic = true)
    private Map<String, String> tags;

    @Schema(
        title = "The access tier of the uploaded blob.",
        description = "The operation is allowed on a page blob in a premium Storage Account or a block blob in a blob " +
            "Storage Account or GPV2 Account. A premium page blob's tier determines the allowed size, IOPS, and bandwidth " +
            "of the blob. A block blob's tier determines the Hot/Cool/Archive storage type. " +
            "This does not update the blob's etag."
    )
    @PluginProperty(dynamic = false)
    private AccessTier accessTier;

    @Schema(
        title = "Sets a legal hold on the blob.",
        description = "NOTE: Blob Versioning must be enabled on your storage account and the blob must be in a container" +
            " with immutable storage with versioning enabled to call this API."
    )
    @PluginProperty(dynamic = false)
    private Boolean legalHold;

    private BlobImmutabilityPolicy immutabilityPolicy;

    @Override
    public Upload.Output run(RunContext runContext) throws Exception {
        URI from = new URI(runContext.render(this.from));

        BlobClient blobClient = this.blobClient(runContext);

        if (this.metadata != null) {
            blobClient.setMetadata(this.metadata.entrySet()
                .stream()
                .map(throwFunction(flow -> new AbstractMap.SimpleEntry<>(
                    runContext.render(flow.getKey()),
                    runContext.render(flow.getValue())
                )))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
            );
        }

        if (this.tags != null) {
            blobClient.setTags(this.tags.entrySet()
                .stream()
                .map(throwFunction(flow -> new AbstractMap.SimpleEntry<>(
                    runContext.render(flow.getKey()),
                    runContext.render(flow.getValue())
                )))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
            );
        }

        if (this.accessTier != null) {
            blobClient.setAccessTier(com.azure.storage.blob.models.AccessTier.fromString(this.accessTier.name()));
        }

        if (this.accessTier != null) {
            blobClient.setLegalHold(this.legalHold);
        }

        if (this.immutabilityPolicy != null) {
            blobClient.setImmutabilityPolicy(this.immutabilityPolicy.to(runContext));
        }

        runContext.logger().debug("Upload from '{}' to '{}'", from, blobClient.getBlobName());

        try (var is = runContext.storage().getFile(from)) {
            blobClient.upload(is, true);
        }

        runContext.metric(Counter.of("file.size", blobClient.getProperties().getBlobSize()));

        return Output
            .builder()
            .blob(Blob.of(blobClient, blobClient.getProperties()))
            .build();

    }

    @SuperBuilder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The uploaded blob."
        )
        private final Blob blob;
    }
}
