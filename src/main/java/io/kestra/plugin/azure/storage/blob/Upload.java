package io.kestra.plugin.azure.storage.blob;

import com.azure.storage.blob.BlobClient;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.storage.blob.abstracts.AbstractBlobStorageWithSasObject;
import io.kestra.plugin.azure.storage.blob.models.AccessTier;
import io.kestra.plugin.azure.storage.blob.models.Blob;
import io.kestra.plugin.azure.storage.blob.models.BlobImmutabilityPolicy;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
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
            title = "Upload an input file to Azure Blob Storage",
            code = """
                id: azure_storage_blob_upload
                namespace: company.team

                inputs:
                  - id: myfile
                    type: FILE

                tasks:
                  - id: upload
                    type: io.kestra.plugin.azure.storage.blob.Upload
                    endpoint: https://kestra.blob.core.windows.net
                    connectionString: "{{ secret('AZURE_CONNECTION_STRING') }}"
                    container: kestra
                    from: "{{ inputs.myfile }}"
                    name: "myblob"
                """
        ),
        @Example(
            full = true,
            title = "Extract data via an HTTP API call and upload it as a file to Azure Blob Storage",
            code = """
                id: azure_blob_upload
                namespace: company.team

                tasks:
                  - id: extract
                    type: io.kestra.plugin.core.http.Download
                    uri: https://huggingface.co/datasets/kestra/datasets/raw/main/csv/salaries.csv

                  - id: load
                    type: io.kestra.plugin.azure.storage.blob.Upload
                    endpoint: https://kestra.blob.core.windows.net
                    connectionString: "{{ secret('AZURE_CONNECTION_STRING') }}"
                    container: kestra
                    from: "{{ outputs.extract.uri }}"
                    name: data.csv
            """
        )
    }
)
@Schema(
    title = "Upload a file to Azure Blob Storage container."
)
public class Upload extends AbstractBlobStorageWithSasObject implements RunnableTask<Upload.Output> {
    @Schema(
        title = "The file from the internal storage to upload to the Azure Blob Storage."
    )
    @PluginProperty(internalStorageURI = true)
    @NotNull
    private Property<String> from;

    @Schema(
        title = "Metadata for the blob."
    )
    private Property<Map<String, String>> metadata;

    @Schema(
        title = "User defined tags."
    )
    private Property<Map<String, String>> tags;

    @Schema(
        title = "The access tier of the uploaded blob.",
        description = "The operation is allowed on a page blob in a premium Storage Account or a block blob in a blob " +
            "Storage Account or GPV2 Account. A premium page blob's tier determines the allowed size, IOPS, and bandwidth " +
            "of the blob. A block blob's tier determines the Hot/Cool/Archive storage type. " +
            "This does not update the blob's etag."
    )
    private Property<AccessTier> accessTier;

    @Schema(
        title = "Sets a legal hold on the blob.",
        description = "NOTE: Blob Versioning must be enabled on your storage account and the blob must be in a container" +
            " with immutable storage with versioning enabled to call this API."
    )
    private Property<Boolean> legalHold;

    private BlobImmutabilityPolicy immutabilityPolicy;

    @Override
    public Upload.Output run(RunContext runContext) throws Exception {
        URI from = new URI(runContext.render(this.from).as(String.class).orElseThrow());

        BlobClient blobClient = this.blobClient(runContext);

        if (this.metadata != null) {
            blobClient.setMetadata(runContext.render(this.metadata).asMap(String.class, String.class)
                .entrySet()
                .stream()
                .map(throwFunction(flow -> new AbstractMap.SimpleEntry<>(
                    runContext.render(flow.getKey()),
                    runContext.render(flow.getValue())
                )))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
            );
        }

        if (this.tags != null) {
            blobClient.setTags(runContext.render(this.tags).asMap(String.class, String.class)
                .entrySet()
                .stream()
                .map(throwFunction(flow -> new AbstractMap.SimpleEntry<>(
                    runContext.render(flow.getKey()),
                    runContext.render(flow.getValue())
                )))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
            );
        }

        if (this.accessTier != null) {
            blobClient.setAccessTier(com.azure.storage.blob.models.AccessTier.fromString(runContext.render(this.accessTier).as(AccessTier.class).orElseThrow().name()));
        }

        if (this.accessTier != null) {
            blobClient.setLegalHold(runContext.render(this.legalHold).as(Boolean.class).orElseThrow());
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
