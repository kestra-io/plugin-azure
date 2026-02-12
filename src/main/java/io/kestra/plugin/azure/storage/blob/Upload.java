package io.kestra.plugin.azure.storage.blob;

import com.azure.storage.blob.BlobClient;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.Metric;
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
    },
    metrics = {
        @Metric(name = "file.size", type = Counter.TYPE, description = "The size of the uploaded blob, in bytes.")
    }
)
@Schema(
    title = "Upload a file to Azure Blob Storage",
    description = "Uploads a file from Kestra internal storage to a blob, applying optional metadata, tags, access tier, legal hold, and immutability policy. Overwrites if the blob exists."
)
public class Upload extends AbstractBlobStorageWithSasObject implements RunnableTask<Upload.Output> {
    @Schema(title = "Source file", description = "kestra:// URI from internal storage to upload")
    @PluginProperty(internalStorageURI = true)
    @NotNull
    private Property<String> from;

    @Schema(title = "Blob metadata", description = "Key/value metadata to set on the blob")
    private Property<Map<String, String>> metadata;

    @Schema(title = "Blob tags", description = "User-defined tags to apply")
    private Property<Map<String, String>> tags;

    @Schema(
        title = "Access tier",
        description = "Hot/Cool/Archive for block blobs or premium tiers for page blobs; does not change etag"
    )
    private Property<AccessTier> accessTier;

    @Schema(
        title = "Legal hold",
        description = "Sets legal hold; requires container immutable storage with versioning enabled"
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
            title = "Uploaded blob"
        )
        private final Blob blob;
    }
}
