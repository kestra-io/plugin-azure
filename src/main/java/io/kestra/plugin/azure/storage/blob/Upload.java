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
@Plugin(examples = {
    @Example(full = true, title = "Upload an input file to Azure Blob Storage", code = """
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
                """),
    @Example(full = true, title = "Extract data via an HTTP API call and upload it as a file to Azure Blob Storage", code = """
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
                """)
}, metrics = {
<<<<<<< HEAD
        @Metric(name = "file.size", type = Counter.TYPE, description = "The size of the uploaded blob, in bytes.")
<<<<<<< HEAD
    }
)
@Schema(
    title = "Upload a file to Azure Blob Storage",
    description = "Uploads a file from Kestra internal storage to a blob, applying optional metadata, tags, access tier, legal hold, and immutability policy. Overwrites if the blob exists."
)
public class Upload extends AbstractBlobStorageWithSasObject implements RunnableTask<Upload.Output> {
    @Schema(title = "Source file", description = "kestra:// URI from internal storage to upload")
=======
=======
    @Metric(name = "file.size", type = Counter.TYPE, description = "The size of the uploaded blob, in bytes.")
>>>>>>> 5ac43f5 (Fix Upload task for directory uploads, implement Copilot suggestions, and add UploadTest for coverage)
})
@Schema(title = "Upload a file to Azure Blob Storage container.")
public class Upload extends AbstractBlobStorageWithSasObject implements RunnableTask<Upload.Output> {

    @Schema(title = "The file from the internal storage to upload to the Azure Blob Storage.")
>>>>>>> 1411081 (Fix Upload task to support single and directory uploads safely)
    @PluginProperty(internalStorageURI = true)
    @NotNull
    private Property<String> from;

<<<<<<< HEAD
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
=======
    @Schema(title = "Metadata for the blob.")
    private Property<Map<String, String>> metadata;

    @Schema(title = "User defined tags.")
    private Property<Map<String, String>> tags;

    @Schema(title = "The access tier of the uploaded blob.", description = "The operation is allowed on a page blob in a premium Storage Account or a block blob in a blob "
            + "Storage Account or GPV2 Account. A premium page blob's tier determines the allowed size, IOPS, and bandwidth "
            + "of the blob. A block blob's tier determines the Hot/Cool/Archive storage type. "
            + "This does not update the blob's etag.")
    private Property<AccessTier> accessTier;

    @Schema(title = "Sets a legal hold on the blob.", description = "NOTE: Blob Versioning must be enabled on your storage account and the blob must be in a container"
<<<<<<< HEAD
            +
            " with immutable storage with versioning enabled to call this API.")
>>>>>>> 1411081 (Fix Upload task to support single and directory uploads safely)
=======
            + " with immutable storage with versioning enabled to call this API.")
>>>>>>> 5ac43f5 (Fix Upload task for directory uploads, implement Copilot suggestions, and add UploadTest for coverage)
    private Property<Boolean> legalHold;

    private BlobImmutabilityPolicy immutabilityPolicy;

    @Override
    public Upload.Output run(RunContext runContext) throws Exception {
        URI fromUri = new URI(runContext.render(this.from).as(String.class).orElseThrow());
        BlobClient baseClient = this.blobClient(runContext);

        List<Blob> uploadedBlobs = new ArrayList<>();
        if (fromUri.toString().endsWith("/")) {
            var containerClient = baseClient.getContainerClient();
            String baseBlobName = baseClient.getBlobName();
            if (baseBlobName == null) {
                baseBlobName = "";
            }

            String directoryPath = runContext.render(this.from).as(String.class).orElseThrow();
            List<URI> files = runContext.storage().list(directoryPath).stream()
                    .filter(u -> !u.toString().endsWith("/"))
                    .collect(Collectors.toList());

            runContext.logger().debug(
                    "Uploading {} files from '{}' to container '{}'",
                    files.size(),
                    fromUri,
                    containerClient.getBlobContainerName()
            );

            for (URI fileUri : files) {
                String relativePath = Paths.get(fromUri.getPath()).relativize(Paths.get(fileUri.getPath())).toString();
                String targetBlobName;
                if (baseBlobName.isEmpty()) {
                    targetBlobName = relativePath;
                } else if (baseBlobName.endsWith("/")) {
                    targetBlobName = baseBlobName + relativePath;
                } else {
                    targetBlobName = baseBlobName + "/" + relativePath;
                }
                BlobClient blobClient = containerClient.getBlobClient(targetBlobName.replace("\\", "/"));
                uploadedBlobs.add(uploadSingleFile(runContext, fileUri, blobClient));
            }

            if (uploadedBlobs.isEmpty()) {
                throw new IllegalArgumentException("No files found under " + fromUri);
            }
        } else {
            uploadedBlobs.add(uploadSingleFile(runContext, fromUri, baseClient));
        }

        return Output.builder()
                .blob(uploadedBlobs.size() == 1 ? uploadedBlobs.get(0) : null)
                .blobs(uploadedBlobs)
                .build();
    }

    private Blob uploadSingleFile(RunContext runContext, URI fileUri, BlobClient blobClient) throws Exception {
        runContext.logger().debug("Uploading blob '{}'", blobClient.getBlobName());

        if (this.metadata != null) {
            blobClient.setMetadata(
                    runContext.render(this.metadata)
                            .asMap(String.class, String.class)
                            .entrySet()
                            .stream()
                            .map(throwFunction(entry -> new AbstractMap.SimpleEntry<>(
                            runContext.render(entry.getKey()),
                            runContext.render(entry.getValue()))))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
        }

        if (this.tags != null) {
            blobClient.setTags(
                    runContext.render(this.tags)
                            .asMap(String.class, String.class)
                            .entrySet()
                            .stream()
                            .map(throwFunction(entry -> new AbstractMap.SimpleEntry<>(
                            runContext.render(entry.getKey()),
                            runContext.render(entry.getValue()))))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
        }

        if (this.accessTier != null) {
            blobClient.setAccessTier(
                    com.azure.storage.blob.models.AccessTier.fromString(
                            runContext.render(this.accessTier)
                                    .as(AccessTier.class)
                                    .orElseThrow()
                                    .name()));
        }

        if (this.legalHold != null) {
            blobClient.setLegalHold(
                    runContext.render(this.legalHold).as(Boolean.class).orElseThrow());
        }

        if (this.immutabilityPolicy != null) {
            blobClient.setImmutabilityPolicy(this.immutabilityPolicy.to(runContext));
        }

        runContext.metric(
                Counter.of("file.size", blobClient.getProperties().getBlobSize()));

        return Blob.of(blobClient, blobClient.getProperties());
    }

    @SuperBuilder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
<<<<<<< HEAD
        @Schema(
            title = "Uploaded blob"
        )
=======

<<<<<<< HEAD
        @Schema(title = "The uploaded blob (single file upload only).")
>>>>>>> 1411081 (Fix Upload task to support single and directory uploads safely)
=======
        @Schema(title = "The uploaded blob (single file upload only).",
                description = "Present only when a single file is uploaded; will be null when uploading a directory (see 'blobs').")
>>>>>>> 5ac43f5 (Fix Upload task for directory uploads, implement Copilot suggestions, and add UploadTest for coverage)
        private final Blob blob;

        @Schema(title = "The uploaded blobs (directory upload).")
        private final List<Blob> blobs;
    }
}
