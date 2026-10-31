package io.kestra.plugin.azure.storage.blob;

import java.net.URI;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.options.BlobParallelUploadOptions;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Data;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.shared.storage.blob.models.AccessTier;
import io.kestra.plugin.azure.shared.storage.blob.models.Blob;
import io.kestra.plugin.azure.shared.storage.blob.models.BlobImmutabilityPolicy;
import io.kestra.plugin.azure.shared.storage.blob.abstracts.AbstractBlobStorageWithSasObject;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;

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
    @PluginProperty(internalStorageURI = true, group = "main")
    @NotNull
    private Object from;

    @Schema(title = "Blob metadata", description = "Key/value metadata to set on the blob")
    @PluginProperty(group = "advanced")
    private Property<Map<String, String>> metadata;

    @Schema(title = "Blob tags", description = "User-defined tags to apply")
    @PluginProperty(group = "advanced")
    private Property<Map<String, String>> tags;

    @Schema(
        title = "Access tier",
        description = "Hot/Cool/Archive for block blobs or premium tiers for page blobs; does not change etag"
    )
    @PluginProperty(group = "advanced")
    private Property<AccessTier> accessTier;

    @Schema(
        title = "Legal hold",
        description = "Sets legal hold; requires container immutable storage with versioning enabled"
    )
    @PluginProperty(group = "advanced")
    private Property<Boolean> legalHold;

    @Schema(title = "Immutability policy to apply to the uploaded blob")
    @PluginProperty(group = "advanced")
    private BlobImmutabilityPolicy immutabilityPolicy;

    @Schema(
        title = "Validate checksum",
        description = "Compute MD5 client-side and pass it to Azure for server-side integrity verification; on mismatch the upload fails with Md5Mismatch"
    )
    @PluginProperty(group = "reliability")
    private Property<Boolean> validateChecksum;

    @Override
    public Upload.Output run(RunContext runContext) throws Exception {
        BlobClient baseClient = this.blobClient(runContext);
        List<Blob> uploadedBlobs = new ArrayList<>();
        boolean rValidateChecksum = runContext.render(this.validateChecksum).as(Boolean.class).orElse(false);

        var rFrom = this.from;
        if (rFrom instanceof Property<?> propertyFrom) {
            @SuppressWarnings("unchecked")
            var fromProperty = (Property<Object>) propertyFrom;
            rFrom = runContext.render(fromProperty).as(Object.class).orElseThrow();
        } else if (rFrom instanceof String stringFrom) {
            rFrom = runContext.render(stringFrom);
        }

        Flux<Map<String, Object>> data;
        if (rFrom instanceof String || rFrom instanceof URI) {
            data = Flux.just(Map.of("uri", rFrom));
        } else if (rFrom instanceof List<?> list) {
            data = Flux.fromIterable(list)
                .map(item -> {
                    if (item instanceof Map<?, ?> map) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> castMap = (Map<String, Object>) map;
                        return castMap;
                    } else if (item instanceof URI || item instanceof String) {
                        return Map.of("uri", item);
                    } else {
                        return Map.of("uri", item.toString());
                    }
                });
        } else if (rFrom instanceof Map<?, ?> map) {
            data = Flux.fromIterable(map.entrySet())
                .map(entry -> Map.of("name", entry.getKey(), "uri", entry.getValue()));
        } else {
            data = Data.from(rFrom).read(runContext);
        }

        List<Map<String, Object>> rows = data.collectList().block();
        if (rows == null || rows.isEmpty()) {
            return Output.builder()
                .blob(null)
                .blobs(Collections.emptyList())
                .build();
        }

        boolean isMultiFile = rows.size() > 1;
        String baseBlobName = baseClient.getBlobName();
        if (baseBlobName == null) {
            baseBlobName = "";
        }

        var containerClient = baseClient.getContainerClient();
        Map<String, URI> filesToUpload = new java.util.LinkedHashMap<>();

        for (Map<String, Object> row : rows) {
            URI fileUri;
            String customName = null;
            if (row.containsKey("uri")) {
                Object uriValue = row.get("uri");
                fileUri = uriValue instanceof URI ? (URI) uriValue : new URI(uriValue.toString());
                if (row.containsKey("name")) {
                    customName = row.get("name").toString();
                }
            } else if (row.size() == 1) {
                Map.Entry<String, Object> entry = row.entrySet().iterator().next();
                Object value = entry.getValue();
                fileUri = value instanceof URI ? (URI) value : new URI(value.toString());
                if (!"uri".equals(entry.getKey())) {
                    customName = entry.getKey();
                }
            } else {
                Object firstValue = row.values().iterator().next();
                fileUri = firstValue instanceof URI ? (URI) firstValue : new URI(firstValue.toString());
            }

            String fileName = customName != null ? customName : java.nio.file.Paths.get(fileUri.getPath()).getFileName().toString();
            String targetBlobName = resolveTargetBlobName(baseBlobName, fileName, isMultiFile);

            if (filesToUpload.containsKey(targetBlobName)) {
                throw new IllegalArgumentException(
                    "Cannot upload files: multiple source URIs resolve to the same destination blob name '%s'. Ensure source file names are distinct or specify unique destination names."
                        .formatted(targetBlobName)
                );
            }
            filesToUpload.put(targetBlobName, fileUri);
        }

        for (Map.Entry<String, URI> entry : filesToUpload.entrySet()) {
            String targetBlobName = entry.getKey();
            URI fileUri = entry.getValue();

            BlobClient blobClient = containerClient.getBlobClient(targetBlobName);

            runContext.logger().debug("Upload from '{}' to '{}'", fileUri, blobClient.getBlobName());

            try (var is = runContext.storage().getFile(fileUri)) {
                blobClient.uploadWithResponse(
                    new BlobParallelUploadOptions(is).setComputeMd5(rValidateChecksum),
                    null,
                    Context.NONE
                );
            }

            if (this.metadata != null) {
                blobClient.setMetadata(
                    runContext.render(this.metadata).asMap(String.class, String.class)
                        .entrySet()
                        .stream()
                        .map(
                            throwFunction(
                                flow -> new AbstractMap.SimpleEntry<>(
                                    runContext.render(flow.getKey()),
                                    runContext.render(flow.getValue())
                                )
                            )
                        )
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
                );
            }

            if (this.tags != null) {
                blobClient.setTags(
                    runContext.render(this.tags).asMap(String.class, String.class)
                        .entrySet()
                        .stream()
                        .map(
                            throwFunction(
                                flow -> new AbstractMap.SimpleEntry<>(
                                    runContext.render(flow.getKey()),
                                    runContext.render(flow.getValue())
                                )
                            )
                        )
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
        }

        return Output
            .builder()
            .blob(uploadedBlobs.size() == 1 ? uploadedBlobs.get(0) : null)
            .blobs(uploadedBlobs)
            .build();
    }

    static String resolveTargetBlobName(String baseBlobName, String fileName, boolean isMultiFile) {
        if (baseBlobName == null) {
            baseBlobName = "";
        }
        if (isMultiFile) {
            if (baseBlobName.isEmpty()) {
                return fileName;
            } else if (baseBlobName.endsWith("/")) {
                return baseBlobName + fileName;
            } else {
                return baseBlobName + "/" + fileName;
            }
        } else {
            if (baseBlobName.isEmpty()) {
                return fileName;
            } else if (baseBlobName.endsWith("/")) {
                return baseBlobName + fileName;
            } else {
                return baseBlobName;
            }
        }
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
