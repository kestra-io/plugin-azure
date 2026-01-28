package io.kestra.plugin.azure.storage.blob;

import java.net.URI;
import java.nio.file.Paths;
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
            title = "Upload multiple files from a working directory to Azure Blob Storage",
            code = """
                id: azure_blob_upload_directory
                namespace: company.team

                tasks:
                  - id: working_dir
                    type: io.kestra.plugin.core.flow.WorkingDirectory
                    tasks:
                      - id: create_files
                        type: io.kestra.plugin.scripts.shell.Commands
                        commands:
                          - mkdir -p data
                          - echo "File 1" > data/file1.txt
                          - echo "File 2" > data/file2.txt
                          - echo "File 3" > data/file3.txt

                  - id: upload_directory
                    type: io.kestra.plugin.azure.storage.blob.Upload
                    endpoint: https://kestra.blob.core.windows.net
                    connectionString: "{{ secret('AZURE_CONNECTION_STRING') }}"
                    container: kestra
                    from: "{{ outputs.working_dir.outputFiles }}"
                    name: "uploads/"
                """
        ),
        @Example(
            full = true,
            title = "Upload multiple files from a list to Azure Blob Storage",
            code = """
                id: azure_blob_upload_list
                namespace: company.team

                tasks:
                  - id: download_files
                    type: io.kestra.plugin.core.http.Download
                    uri: https://example.com/file1.csv

                  - id: download_more
                    type: io.kestra.plugin.core.http.Download
                    uri: https://example.com/file2.csv

                  - id: upload_multiple
                    type: io.kestra.plugin.azure.storage.blob.Upload
                    endpoint: https://kestra.blob.core.windows.net
                    connectionString: "{{ secret('AZURE_CONNECTION_STRING') }}"
                    container: kestra
                    from:
                      - "{{ outputs.download_files.uri }}"
                      - "{{ outputs.download_more.uri }}"
                    name: "uploads/"
                """
        )
    },
    metrics = {
        @Metric(name = "file.size", type = Counter.TYPE, description = "The size of the uploaded blob, in bytes.")
    }
)
@Schema(
    title = "Upload one or multiple files to Azure Blob Storage container.",
    description = "This task supports uploading a single file, multiple files from a map (e.g., from WorkingDirectory outputs), or multiple files from a list."
)
public class Upload extends AbstractBlobStorageWithSasObject implements RunnableTask<Upload.Output> {
    @Schema(
        title = "The file(s) from internal storage to upload to Azure Blob Storage.",
        description = "Can be:\n" +
            "- A single file URI (string): `\"{{ outputs.task.uri }}\"`\n" +
            "- A map of files (from WorkingDirectory or similar tasks): `\"{{ outputs.workingdir.outputFiles }}\"`\n" +
            "- A list of file URIs: `[\"kestra:///file1.txt\", \"kestra:///file2.txt\"]`\n\n" +
            "When uploading multiple files, the 'name' property should typically end with '/' to indicate a directory prefix."
    )
    @PluginProperty(internalStorageURI = true)
    @NotNull
    private Property<Object> from;

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
        Object fromValue = runContext.render(this.from).as(Object.class).orElseThrow();
        BlobClient baseClient = this.blobClient(runContext);
        var containerClient = baseClient.getContainerClient();

        List<Blob> uploadedBlobs = new ArrayList<>();

        if (fromValue instanceof String) {
            String fromString = (String) fromValue;
            URI fromUri = new URI(fromString);
            
            uploadedBlobs.add(uploadSingleFile(runContext, fromUri, baseClient));
            
        } else if (fromValue instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> filesMap = (Map<String, Object>) fromValue;
            
            String baseBlobName = baseClient.getBlobName();
            if (baseBlobName == null) {
                baseBlobName = "";
            }

            runContext.logger().debug(
                "Uploading {} files to container '{}' with base name '{}'",
                filesMap.size(),
                containerClient.getBlobContainerName(),
                baseBlobName
            );

            for (Map.Entry<String, Object> entry : filesMap.entrySet()) {
                String fileName = entry.getKey();
                URI fileUri = new URI(entry.getValue().toString());
                
                String targetBlobName;
                if (baseBlobName.isEmpty()) {
                    targetBlobName = fileName;
                } else if (baseBlobName.endsWith("/")) {
                    targetBlobName = baseBlobName + fileName;
                } else {
                    targetBlobName = baseBlobName + "/" + fileName;
                }
                
                BlobClient blobClient = containerClient.getBlobClient(targetBlobName.replace("\\", "/"));
                uploadedBlobs.add(uploadSingleFile(runContext, fileUri, blobClient));
            }

            if (uploadedBlobs.isEmpty()) {
                throw new IllegalArgumentException("No files found in the provided map");
            }
            
        } else if (fromValue instanceof List) {
            // Multiple files from a list
            @SuppressWarnings("unchecked")
            List<?> filesList = (List<?>) fromValue;
            
            String baseBlobName = baseClient.getBlobName();
            if (baseBlobName == null) {
                baseBlobName = "";
            }

            runContext.logger().debug(
                "Uploading {} files to container '{}' with base name '{}'",
                filesList.size(),
                containerClient.getBlobContainerName(),
                baseBlobName
            );

            for (Object fileObj : filesList) {
                URI fileUri = new URI(fileObj.toString());
                String fileName = Paths.get(fileUri.getPath()).getFileName().toString();
                
                String targetBlobName;
                if (baseBlobName.isEmpty()) {
                    targetBlobName = fileName;
                } else if (baseBlobName.endsWith("/")) {
                    targetBlobName = baseBlobName + fileName;
                } else {
                    targetBlobName = baseBlobName + "/" + fileName;
                }
                
                BlobClient blobClient = containerClient.getBlobClient(targetBlobName.replace("\\", "/"));
                uploadedBlobs.add(uploadSingleFile(runContext, fileUri, blobClient));
            }

            if (uploadedBlobs.isEmpty()) {
                throw new IllegalArgumentException("No files found in the provided list");
            }
            
        } else {
            throw new IllegalArgumentException(
                "The 'from' property must be a String (single file URI), Map (multiple files), or List (multiple file URIs). " +
                "Got: " + fromValue.getClass().getName()
            );
        }

        return Output.builder()
            .blob(uploadedBlobs.size() == 1 ? uploadedBlobs.get(0) : null)
            .blobs(uploadedBlobs)
            .build();
    }

    private Blob uploadSingleFile(RunContext runContext, URI fileUri, BlobClient blobClient) throws Exception {
        runContext.logger().debug("Uploading from '{}' to '{}'", fileUri, blobClient.getBlobName());

        try (var is = runContext.storage().getFile(fileUri)) {
            blobClient.upload(is, true);
        }

        if (this.metadata != null) {
            blobClient.setMetadata(runContext.render(this.metadata).asMap(String.class, String.class)
                .entrySet()
                .stream()
                .map(throwFunction(entry -> new AbstractMap.SimpleEntry<>(
                    runContext.render(entry.getKey()),
                    runContext.render(entry.getValue())
                )))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
            );
        }

        if (this.tags != null) {
            blobClient.setTags(runContext.render(this.tags).asMap(String.class, String.class)
                .entrySet()
                .stream()
                .map(throwFunction(entry -> new AbstractMap.SimpleEntry<>(
                    runContext.render(entry.getKey()),
                    runContext.render(entry.getValue())
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

        return Blob.of(blobClient, blobClient.getProperties());
    }

    @SuperBuilder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The uploaded blob (single file upload only).",
            description = "Present only when a single file is uploaded; will be null when uploading multiple files (see 'blobs')."
        )
        private final Blob blob;

        @Schema(
            title = "List of all uploaded blobs.",
            description = "Contains all uploaded blobs. For single file uploads, this list will contain one item."
        )
        private final List<Blob> blobs;
    }
}
