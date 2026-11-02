package io.kestra.plugin.azure.storage.blob;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.azure.core.util.Context;
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
        description = Data.From.DESCRIPTION + """


            Each file is uploaded under `name` + `/` + its file name when several files are uploaded or when `name` ends with `/`.
            Accepted shapes:
            - A single `kestra://` URI, or a list of URIs.
            - A map with a `uri` key (or a single value), or a list of such maps: one file per item.
            - A map of file names to `kestra://` URIs with no `uri` key, such as `{{ outputs.previous_task.outputFiles }}`: one file per entry, named after its key.
            Two files resolving to the same blob name are rejected before anything is uploaded."""
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
        var baseClient = this.blobClient(runContext);
        var uploadedBlobs = new ArrayList<Blob>();
        var rValidateChecksum = runContext.render(this.validateChecksum).as(Boolean.class).orElse(false);

        var sources = resolveSources(runContext, this.from);
        if (sources.isEmpty()) {
            return Output.builder()
                .blob(null)
                .blobs(Collections.emptyList())
                .build();
        }

        var isMultiFile = sources.size() > 1;
        var baseBlobName = Optional.ofNullable(baseClient.getBlobName()).orElse("");
        var containerClient = baseClient.getContainerClient();
        var filesToUpload = new LinkedHashMap<String, URI>();

        for (var source : sources) {
            var targetBlobName = resolveTargetBlobName(baseBlobName, source.fileName(), isMultiFile);
            if (filesToUpload.putIfAbsent(targetBlobName, source.uri()) != null) {
                throw new IllegalArgumentException(
                    "Cannot upload files: multiple source URIs resolve to the same destination blob name '%s'. Ensure source file names are distinct or use a map of unique file names to URIs."
                        .formatted(targetBlobName)
                );
            }
        }

        for (var entry : filesToUpload.entrySet()) {
            var fileUri = entry.getValue();

            var blobClient = containerClient.getBlobClient(entry.getKey());

            runContext.logger().debug("Upload from '{}' to '{}'", fileUri, blobClient.getBlobName());

            try (var is = runContext.storage().getFile(fileUri)) {
                blobClient.uploadWithResponse(
                    new BlobParallelUploadOptions(is).setComputeMd5(rValidateChecksum),
                    null,
                    Context.NONE
                );
            }

            if (this.metadata != null) {
                blobClient.setMetadata(runContext.render(this.metadata).asMap(String.class, String.class));
            }

            if (this.tags != null) {
                blobClient.setTags(runContext.render(this.tags).asMap(String.class, String.class));
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

            var blob = Blob.of(blobClient, blobClient.getProperties());
            uploadedBlobs.add(blob);
        }

        return Output
            .builder()
            .blob(uploadedBlobs.size() == 1 ? uploadedBlobs.get(0) : null)
            .blobs(uploadedBlobs)
            .build();
    }

    private List<Source> resolveSources(RunContext runContext, Object rawFrom) throws Exception {
        var rFrom = rawFrom;
        // Values that are already rendered, or parsed out of a rendered string, must never be rendered a second time.
        var needsRendering = false;
        if (rFrom instanceof Property<?> propertyFrom) {
            @SuppressWarnings("unchecked")
            var fromProperty = (Property<Object>) propertyFrom;
            rFrom = runContext.render(fromProperty).as(Object.class).orElseThrow(() -> new IllegalArgumentException("'from' rendered to an empty value"));
        } else if (rFrom instanceof String stringFrom) {
            rFrom = parseJsonIfNeeded(runContext.render(stringFrom));
        } else {
            needsRendering = true;
        }

        var sources = new ArrayList<Source>();
        switch (rFrom) {
            case String str -> sources.add(Source.of(str));
            case URI uri -> sources.add(Source.of(uri.toString()));
            case List<?> list -> {
                for (var item : list) {
                    switch (item) {
                        case String str -> sources.add(Source.of(needsRendering ? runContext.render(str) : str));
                        case URI uri -> sources.add(Source.of(uri.toString()));
                        case Map<?, ?> map -> sources.add(Source.ofRow(resolveMap(runContext, map, needsRendering)));
                        default -> throw new IllegalArgumentException("Unsupported item in 'from' list: " + item);
                    }
                }
            }
            case Map<?, ?> map -> {
                var rMap = resolveMap(runContext, map, needsRendering);
                if (isFileNameToUriMap(rMap)) {
                    rMap.forEach((fileName, uri) -> sources.add(new Source(toUri(uri), fileName)));
                } else {
                    sources.add(Source.ofRow(rMap));
                }
            }
            default -> {
                var rows = Data.from(rFrom).read(runContext).collectList().block();
                if (rows != null) {
                    rows.forEach(row -> sources.add(Source.ofRow(row)));
                }
            }
        }
        return sources;
    }

    private static Object parseJsonIfNeeded(String rFrom) {
        var trimmed = rFrom.trim();
        if (!trimmed.startsWith("{") && !trimmed.startsWith("[")) {
            return rFrom;
        }
        try {
            return JacksonMapper.ofJson().readValue(trimmed, Object.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("'from' looks like JSON but could not be parsed: " + e.getMessage(), e);
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> resolveMap(RunContext runContext, Map<?, ?> map, boolean needsRendering) throws Exception {
        if (needsRendering) {
            return runContext.render((Map<String, Object>) map);
        }
        var copy = new LinkedHashMap<String, Object>();
        map.forEach((key, value) -> copy.put(String.valueOf(key), value));
        return copy;
    }

    // The `outputFiles` shape: no `uri` key and every value is an internal storage URI.
    private static boolean isFileNameToUriMap(Map<String, Object> map) {
        return !map.isEmpty()
            && !map.containsKey("uri")
            && map.values().stream().allMatch(value -> value != null && value.toString().startsWith("kestra://"));
    }

    private static URI toUri(Object value) {
        return value instanceof URI uri ? uri : URI.create(String.valueOf(value));
    }

    static String resolveTargetBlobName(String baseBlobName, String fileName, boolean isMultiFile) {
        if (baseBlobName == null || baseBlobName.isEmpty()) {
            return fileName;
        }
        if (baseBlobName.endsWith("/")) {
            return baseBlobName + fileName;
        }
        return isMultiFile ? baseBlobName + "/" + fileName : baseBlobName;
    }

    private record Source(URI uri, String fileName) {
        static Source of(String uri) {
            var parsed = toUri(uri);
            var path = parsed.getPath() == null ? "" : parsed.getPath();
            var fileName = path.substring(path.lastIndexOf('/') + 1);
            if (fileName.isEmpty()) {
                throw new IllegalArgumentException("Cannot derive a file name from URI '" + uri + "'. Use a map of file names to URIs to name it explicitly.");
            }
            return new Source(parsed, fileName);
        }

        static Source ofRow(Map<String, Object> row) {
            if (row.isEmpty()) {
                throw new IllegalArgumentException("Empty item in 'from': expected a `uri` entry or a single URI value");
            }
            var value = row.containsKey("uri") ? row.get("uri") : row.values().iterator().next();
            return of(String.valueOf(value));
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
