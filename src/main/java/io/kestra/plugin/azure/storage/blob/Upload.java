package io.kestra.plugin.azure.storage.blob;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
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
import io.kestra.core.serializers.JacksonMapper;
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
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
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
        if (this.from == null) {
            return Output.builder().blobs(List.of()).build();
        }

        BlobClient baseClient = this.blobClient(runContext);
        List<Blob> uploadedBlobs = new ArrayList<>();
        boolean rValidateChecksum = runContext.render(this.validateChecksum).as(Boolean.class).orElse(false);

        var rFrom = this.from;
        if (rFrom instanceof Property<?> propertyFrom) {
            @SuppressWarnings("unchecked")
            var fromProperty = (Property<Object>) propertyFrom;
            rFrom = runContext.render(fromProperty).as(Object.class).orElse(null);
        } else if (rFrom instanceof String stringFrom) {
            rFrom = runContext.render(stringFrom);
        }

        if (rFrom == null) {
            return Output.builder().blobs(List.of()).build();
        }

        if (rFrom instanceof Collection<?> col && col.isEmpty()) {
            return Output.builder().blobs(List.of()).build();
        }
        if (rFrom instanceof Map<?, ?> map && map.isEmpty()) {
            return Output.builder().blobs(List.of()).build();
        }

        if (rFrom instanceof String str) {
            String trimmed = str.trim();
            if (trimmed.startsWith("[") || trimmed.startsWith("{")) {
                try {
                    rFrom = JacksonMapper.ofJson().readValue(trimmed, Object.class);
                } catch (Exception ignored) {
                    // Not valid JSON, keep as raw string
                }
            }
        }

        boolean isMultiple = (rFrom instanceof Collection || rFrom instanceof Map);
        List<UploadItem> items = new ArrayList<>();

        if (rFrom instanceof Map<?, ?> fromMap) {
            if (fromMap.containsKey("uri")) {
                URI uri = toUri(fromMap.get("uri"));
                String fileName = fromMap.containsKey("name") && fromMap.get("name") != null
                    ? fromMap.get("name").toString()
                    : extractFileName(uri);
                items.add(new UploadItem(uri, fileName));
            } else {
                for (Map.Entry<?, ?> entry : fromMap.entrySet()) {
                    String fileName = entry.getKey().toString();
                    URI uri = toUri(entry.getValue());
                    items.add(new UploadItem(uri, fileName));
                }
            }
        } else if (rFrom instanceof Collection<?> collection) {
            for (Object elem : collection) {
                if (elem instanceof Map<?, ?> mapElem) {
                    URI uri = extractUri(mapElem);
                    String fileName = mapElem.containsKey("name") && mapElem.get("name") != null
                        ? mapElem.get("name").toString()
                        : extractFileName(uri);
                    items.add(new UploadItem(uri, fileName));
                } else if (elem != null) {
                    URI uri = toUri(elem);
                    String fileName = extractFileName(uri);
                    items.add(new UploadItem(uri, fileName));
                }
            }
        } else if (rFrom instanceof URI uri) {
            items.add(new UploadItem(uri, extractFileName(uri)));
        } else if (rFrom instanceof String str) {
            URI uri = toUri(str);
            items.add(new UploadItem(uri, extractFileName(uri)));
        } else {
            isMultiple = true;
            var flux = Data.from(rFrom).read(runContext);
            var rows = flux.collectList().block();
            if (rows != null) {
                for (var row : rows) {
                    URI uri = extractUri(row);
                    String fileName = row.containsKey("name") && row.get("name") != null
                        ? row.get("name").toString()
                        : extractFileName(uri);
                    items.add(new UploadItem(uri, fileName));
                }
            }
        }

        if (items.isEmpty()) {
            return Output.builder().blobs(List.of()).build();
        }

        if (items.size() > 1) {
            isMultiple = true;
        }

        BlobContainerClient containerClient = baseClient.getContainerClient();
        String baseBlobName = baseClient.getBlobName();
        if (baseBlobName == null) {
            baseBlobName = "";
        }

        Set<String> targetBlobNames = new HashSet<>();
        List<TargetItem> targets = new ArrayList<>(items.size());

        for (UploadItem item : items) {
            String targetBlobName = computeTargetBlobName(baseBlobName, item.fileName(), isMultiple);
            if (!targetBlobNames.add(targetBlobName)) {
                throw new IllegalArgumentException(
                    String.format(
                        "Duplicate destination blob '%s' detected for source '%s'. Multiple files cannot be uploaded to the same blob name.",
                        targetBlobName, item.uri()
                    )
                );
            }
            targets.add(new TargetItem(item, targetBlobName));
        }

        for (TargetItem target : targets) {
            UploadItem item = target.item();
            String targetBlobName = target.targetBlobName();

            BlobClient blobClient = targetBlobName.equals(baseBlobName)
                ? baseClient
                : containerClient.getBlobClient(targetBlobName);

            runContext.logger().debug("Upload from '{}' to '{}'", item.uri(), blobClient.getBlobName());

            try (var is = runContext.storage().getFile(item.uri())) {
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

    static String computeTargetBlobName(String baseBlobName, String fileName, boolean isMultiple) {
        String cleanFileName = fileName != null ? fileName.replace('\\', '/') : "";
        cleanFileName = StringUtils.stripStart(cleanFileName, "/");
        if (baseBlobName == null || baseBlobName.isEmpty()) {
            return cleanFileName;
        }
        if (cleanFileName.isEmpty()) {
            return baseBlobName;
        }
        if (baseBlobName.endsWith("/")) {
            return baseBlobName + cleanFileName;
        }
        if (isMultiple) {
            return baseBlobName + "/" + cleanFileName;
        }
        return baseBlobName;
    }

    private static String extractFileName(URI uri) {
        String path = uri.getPath();
        if (path == null || path.isEmpty()) {
            path = uri.getSchemeSpecificPart();
        }
        if (path != null && !path.isEmpty()) {
            String fileName = FilenameUtils.getName(path);
            if (fileName != null && !fileName.isEmpty()) {
                return fileName;
            }
        }
        return "file";
    }

    private static URI extractUri(Map<?, ?> row) throws URISyntaxException {
        if (row.containsKey("uri")) {
            Object uriValue = row.get("uri");
            return toUri(uriValue);
        } else if (!row.isEmpty()) {
            Object value = row.values().iterator().next();
            return toUri(value);
        }
        throw new IllegalArgumentException("Unable to extract file URI from map: " + row);
    }

    private static URI toUri(Object value) throws URISyntaxException {
        if (value == null) {
            throw new IllegalArgumentException("File URI cannot be null");
        }
        if (value instanceof URI uri) {
            return uri;
        }
        return new URI(value.toString());
    }

    private record UploadItem(URI uri, String fileName) {}

    private record TargetItem(UploadItem item, String targetBlobName) {}

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
