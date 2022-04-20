package io.kestra.plugin.azure.storage.blob;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.storage.blob.abstracts.AbstractBlobStorageObject;
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
            code = {
                "endpoint: \"https://yourblob.blob.core.windows.net\"",
                "connectionString: \"DefaultEndpointsProtocol=...==\"",
                "container: \"mydata\"",
                "from: \"{{ inputs.file }}\"",
                "name: \"myblob\""
            }
        )
    }
)
@Schema(
    title = "Upload a file to a Azure Blob Storage."
)
public class Upload extends AbstractBlobStorageObject implements RunnableTask<Upload.Output> {
    @Schema(
        title = "The file to upload"
    )
    @PluginProperty(dynamic = true)
    private String from;

    @Schema(
        title = "Blob's metadata."
    )
    @PluginProperty(dynamic = true)
    private Map<String, String> metadata;

    @Schema(
        title = "User defined tags."
    )
    @PluginProperty(dynamic = true)
    private Map<String, String> tags;

    @Schema(
        title = "The tier on a blob.",
        description = "The operation is allowed on a page blob in a premium storage account or a block blob in a blob " +
            "storage or GPV2 account. A premium page blob's tier determines the allowed size, IOPS, and bandwidth " +
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

        if (this.accessTier != null) {
            blobClient.setLegalHold(this.legalHold);
        }

        if (this.immutabilityPolicy != null) {
            blobClient.setImmutabilityPolicy(this.immutabilityPolicy.to(runContext));
        }

        runContext.logger().debug("Upload from '{}' to '{}'", from, blobClient.getBlobName());

        blobClient.upload(BinaryData.fromStream(runContext.uriToInputStream(from)), true);

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
            title = "The blob"
        )
        private final Blob blob;
    }
}
