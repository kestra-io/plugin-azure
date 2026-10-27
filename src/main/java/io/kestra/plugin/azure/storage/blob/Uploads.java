package io.kestra.plugin.azure.storage.blob;

import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.azure.shared.storage.blob.abstracts.AbstractBlobStorageWithSas;
import io.kestra.plugin.azure.shared.storage.blob.models.Blob;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.StringUtils;

import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            title = "Upload multiple files to an Azure Blob Storage container",
            full = true,
            code = """
                id: azure_batch_upload
                namespace: company.analytics
                tasks:
                  - id: upload_partitions
                    type: io.kestra.plugin.azure.storage.blob.Uploads
                    endpoint: "https://myaccount.blob.core.windows.net"
                    connectionString: "{{ secret('AZURE_CONNECTION_STRING') }}"
                    container: "analytics-data"
                    prefix: "incoming/daily-batch/"
                    from: "{{ outputs.generate_files.outputFiles }}"
                """
        )
    }
)
@Schema(
    title = "Upload multiple files to Azure Blob Storage.",
    description = "Uploads multiple files from Kestra internal storage to Azure Blob Storage concurrently."
)
public class Uploads extends AbstractBlobStorageWithSas implements RunnableTask<Uploads.Output> {

    @Schema(title = "The files to upload", description = "Can be a list of internal storage URIs or a map of relative path to internal storage URI")
    @PluginProperty(dynamic = true)
    @NotNull
    private Object from;

    @Schema(title = "The blob container name")
    @PluginProperty(dynamic = true)
    @NotNull
    private String container;

    @Schema(title = "The blob prefix", description = "Optional target virtual directory prefix (e.g. raw/data/)")
    @PluginProperty(dynamic = true)
    private String prefix;

    @Schema(title = "Validate checksum", description = "Compute MD5 client-side and pass it to Azure for server-side integrity verification")
    @PluginProperty(dynamic = true)
    private Property<Boolean> validateChecksum;

    @Schema(title = "Blob metadata", description = "User-defined metadata to set on all uploaded blobs")
    @PluginProperty(dynamic = true)
    private Property<Map<String, String>> metadata;

    @Schema(title = "Blob tags", description = "Blob index tags to set on all uploaded blobs")
    @PluginProperty(dynamic = true)
    private Property<Map<String, String>> tags;

    @Override
    public Output run(RunContext runContext) throws Exception {
        BlobServiceClient serviceClient = this.client(runContext);
        String renderedContainer = runContext.render(this.container);
        BlobContainerClient containerClient = serviceClient.getBlobContainerClient(renderedContainer);
        
        String renderedPrefix = runContext.render(this.prefix);
        boolean rValidateChecksum = runContext.render(this.validateChecksum).as(Boolean.class).orElse(false);
        
        Map<String, String> resolvedMetadata = this.metadata != null ? runContext.render(this.metadata).asMap(String.class, String.class) : null;
        Map<String, String> resolvedTags = this.tags != null ? runContext.render(this.tags).asMap(String.class, String.class) : null;

        Map<String, URI> filesToUpload = this.resolveFrom(runContext);

        // Check for duplicate filenames to prevent silent overwrites
        Set<String> targetPaths = new HashSet<>();
        for (String relativePath : filesToUpload.keySet()) {
            if (!targetPaths.add(relativePath)) {
                throw new IllegalArgumentException("Collision detected: Multiple files mapped to the same target path -> " + relativePath);
            }
        }

        List<Blob> uploadedBlobs = Collections.synchronizedList(new ArrayList<>());
        Map<String, URI> outputFiles = Collections.synchronizedMap(new HashMap<>());

        // Upload concurrently using parallel stream
        filesToUpload.entrySet().parallelStream().forEach(entry -> {
            try {
                String targetPath = entry.getKey();
                URI internalUri = entry.getValue();

                String finalBlobName = StringUtils.isEmpty(renderedPrefix) ? targetPath : 
                    (renderedPrefix.endsWith("/") ? renderedPrefix + targetPath : renderedPrefix + "/" + targetPath);

                BlobClient blobClient = containerClient.getBlobClient(finalBlobName);

                try (InputStream is = runContext.storage().getFile(internalUri)) {
                    BlobParallelUploadOptions options = new BlobParallelUploadOptions(is)
                        .setComputeMd5(rValidateChecksum);
                    
                    if (resolvedTags != null) {
                        options.setTags(resolvedTags);
                    }

                    blobClient.uploadWithResponse(options, null, Context.NONE);
                    
                    if (resolvedMetadata != null) {
                        blobClient.setMetadata(resolvedMetadata);
                    }
                }

                Blob blob = Blob.of(blobClient, blobClient.getProperties());
                uploadedBlobs.add(blob);
                outputFiles.put(finalBlobName, internalUri);
                
                runContext.metric(Counter.of("file.size", blobClient.getProperties().getBlobSize(), "blob", finalBlobName));

            } catch (Exception e) {
                throw new RuntimeException("Failed to upload file " + entry.getKey() + ": " + e.getMessage(), e);
            }
        });

        runContext.metric(Counter.of("files.count", uploadedBlobs.size()));

        return Output.builder()
            .blobs(uploadedBlobs)
            .outputFiles(outputFiles)
            .count(uploadedBlobs.size())
            .build();
    }

    @SuppressWarnings("unchecked")
    private Map<String, URI> resolveFrom(RunContext runContext) throws Exception {
        Object renderedFrom = runContext.render(this.from.toString());
        Map<String, URI> result = new HashMap<>();

        if (renderedFrom instanceof String fromStr) {
            renderedFrom = JacksonMapper.toObject(fromStr);
        }

        if (renderedFrom instanceof List<?> list) {
            for (Object item : list) {
                URI uri = new URI(item.toString());
                Path path = Paths.get(uri.getPath());
                result.put(path.getFileName().toString(), uri);
            }
        } else if (renderedFrom instanceof Map<?, ?> map) {
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                result.put(entry.getKey().toString(), new URI(entry.getValue().toString()));
            }
        } else {
            throw new IllegalVariableEvaluationException("Invalid 'from' property type. Expected a List or a Map, but got: " + renderedFrom.getClass().getName());
        }

        return result;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "List of uploaded blobs")
        private List<Blob> blobs;

        @Schema(title = "Map of uploaded blob names to their internal storage URIs")
        private Map<String, URI> outputFiles;

        @Schema(title = "Total number of uploaded files")
        private int count;
    }
}