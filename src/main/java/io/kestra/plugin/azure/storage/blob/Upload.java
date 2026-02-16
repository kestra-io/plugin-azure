package io.kestra.plugin.azure.storage.blob;

import java.net.URI;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.azure.storage.blob.BlobClient;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Data;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import static io.kestra.core.utils.Rethrow.throwFunction;
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
import reactor.core.publisher.Flux;

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
        ),
        @Example(
            full = true,
            title = "Upload multiple files to Azure Blob Storage",
            code = """
                id: azure_blob_upload_multiple
                namespace: company.team

                tasks:
                  - id: upload
                    type: io.kestra.plugin.azure.storage.blob.Upload
                    endpoint: https://kestra.blob.core.windows.net
                    connectionString: "{{ secret('AZURE_CONNECTION_STRING') }}"
                    container: kestra
                    from: "{{ outputs.previous_task.outputFiles }}"
                    name: "uploads/"
            """
        )
    },
    metrics = {
        @Metric(name = "file.size", type = Counter.TYPE, description = "The size of the uploaded blob, in bytes.")
    }
)
@Schema(
    title = "Upload file(s) to Azure Blob Storage",
    description = "Uploads one or more files from Kestra internal storage to Azure Blob Storage."
)
public class Upload extends AbstractBlobStorageWithSasObject implements RunnableTask<Upload.Output>, Data.From {
    @Schema(
        title = Data.From.TITLE,
        description = Data.From.DESCRIPTION
    )
    @PluginProperty(internalStorageURI = true)
    @NotNull
    private Object from;

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
        BlobClient baseClient = this.blobClient(runContext);
        List<Blob> uploadedBlobs = new ArrayList<>();

        var rFrom = this.from;
        if (rFrom instanceof Property<?> propertyFrom) {
            @SuppressWarnings("unchecked")
            var fromProperty = (Property<Object>) propertyFrom;
            rFrom = runContext.render(fromProperty).as(Object.class).orElseThrow();
        }

        var data = (rFrom instanceof String || rFrom instanceof URI)
            ? Flux.just(Map.<String, Object>of("uri", rFrom))
            : Data.from(rFrom).read(runContext);

        data
            .map(throwFunction(row -> {
                // row is Map<String, Object> - extract the URI
                URI fileUri;
                if (row.containsKey("uri")) {
                    Object uriValue = row.get("uri");
                    fileUri = uriValue instanceof URI ? (URI) uriValue : new URI(uriValue.toString());
                } else if (row.size() == 1) {
                    // Single value in map - use it
                    Object value = row.values().iterator().next();
                    fileUri = value instanceof URI ? (URI) value : new URI(value.toString());
                } else {
                    // Try to convert the whole map or take first value
                    Object firstValue = row.values().iterator().next();
                    fileUri = firstValue instanceof URI ? (URI) firstValue : new URI(firstValue.toString());
                }
                
                // Determine which blob client to use
                BlobClient blobClient;
                if (uploadedBlobs.isEmpty()) {
                    // First file uses the base client
                    blobClient = baseClient;
                } else {
                    // Additional files go to container with generated names
                    var containerClient = baseClient.getContainerClient();
                    String baseBlobName = baseClient.getBlobName();
                    if (baseBlobName == null) {
                        baseBlobName = "";
                    }
                    String fileName = java.nio.file.Paths.get(fileUri.getPath()).getFileName().toString();
                    String targetBlobName;
                    if (baseBlobName.isEmpty()) {
                        targetBlobName = fileName;
                    } else if (baseBlobName.endsWith("/")) {
                        targetBlobName = baseBlobName + fileName;
                    } else {
                        targetBlobName = baseBlobName + "/" + fileName;
                    }
                    blobClient = containerClient.getBlobClient(targetBlobName);
                }
                
                runContext.logger().debug("Upload from '{}' to '{}'", fileUri, blobClient.getBlobName());

                try (var is = runContext.storage().getFile(fileUri)) {
                    blobClient.upload(is, true);
                }

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

                if (this.legalHold != null) {
                    blobClient.setLegalHold(runContext.render(this.legalHold).as(Boolean.class).orElseThrow());
                }

                if (this.immutabilityPolicy != null) {
                    blobClient.setImmutabilityPolicy(this.immutabilityPolicy.to(runContext));
                }

                runContext.metric(Counter.of("file.size", blobClient.getProperties().getBlobSize()));

                Blob blob = Blob.of(blobClient, blobClient.getProperties());
                uploadedBlobs.add(blob);
                
                return 1;
            }))
            .reduce(Integer::sum)
            .blockOptional().orElse(0);

        return Output
            .builder()
            .blob(uploadedBlobs.size() == 1 ? uploadedBlobs.get(0) : null)
            .blobs(uploadedBlobs)
            .build();
    }

    @SuperBuilder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Uploaded blob (single file only)",
            description = "Present only when a single file is uploaded; null for multiple files"
        )
        private final Blob blob;

        @Schema(
            title = "All uploaded blobs",
            description = "List of all uploaded blobs"
        )
        private final List<Blob> blobs;
    }
}
