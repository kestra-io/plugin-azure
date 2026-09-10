package io.kestra.plugin.azure.ml;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.azure.core.management.exception.ManagementException;
import com.azure.resourcemanager.machinelearning.MachineLearningManager;
import com.azure.resourcemanager.machinelearning.models.AzureBlobDatastore;
import com.azure.resourcemanager.machinelearning.models.ModelVersion;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;

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
            code = """
                id: azure_ml_download_model
                namespace: company.team

                tasks:
                  - id: download
                    type: io.kestra.plugin.azure.ml.DownloadModel
                    tenantId: "{{ secret('AZURE_TENANT_ID') }}"
                    clientId: "{{ secret('AZURE_CLIENT_ID') }}"
                    clientSecret: "{{ secret('AZURE_CLIENT_SECRET') }}"
                    subscriptionId: "{{ secret('AZURE_SUBSCRIPTION_ID') }}"
                    resourceGroupName: ml-rg
                    workspaceName: ml-workspace
                    modelName: fraud-detector
                    modelVersion: latest
                """
        )
    }
)
@Schema(
    title = "Download an Azure Machine Learning model to Kestra's internal storage",
    description = "Resolves a model version's storage location and downloads its artifact(s) into Kestra's internal storage. A single-file model is stored as-is; a folder-based model is packaged into a single ZIP archive."
)
public class DownloadModel extends AbstractMachineLearningTask implements RunnableTask<DownloadModel.Output> {
    @Schema(title = "Model name")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> modelName;

    @Schema(title = "Model version", description = "Specific version to download, or `latest` (default) to resolve the most recent one")
    @lombok.Builder.Default
    @PluginProperty(group = "main")
    private Property<String> modelVersion = Property.ofValue("latest");

    @Override
    public Output run(RunContext runContext) throws Exception {
        var logger = runContext.logger();
        String rResourceGroupName = rResourceGroupName(runContext);
        String rWorkspaceName = rWorkspaceName(runContext);
        String rModelName = runContext.render(this.modelName).as(String.class).orElseThrow();
        String rVersion = runContext.render(this.modelVersion).as(String.class).filter(v -> !v.isBlank()).orElse("latest");

        MachineLearningManager manager = machineLearningManager(runContext);

        ModelVersion modelVersion;
        try {
            modelVersion = "latest".equalsIgnoreCase(rVersion)
                ? MachineLearningService.latestModelVersion(manager, rResourceGroupName, rWorkspaceName, rModelName)
                : manager.modelVersions().get(rResourceGroupName, rWorkspaceName, rModelName, rVersion);
        } catch (ManagementException e) {
            if (e.getResponse() != null && e.getResponse().getStatusCode() == 404) {
                throw new IllegalArgumentException("Model '%s' version '%s' was not found in workspace '%s'".formatted(rModelName, rVersion, rWorkspaceName), e);
            }
            throw e;
        }
        if (modelVersion == null) {
            throw new IllegalArgumentException("Model '%s' has no registered version in workspace '%s'".formatted(rModelName, rWorkspaceName));
        }

        String modelUri = MachineLearningService.requireModelUri(modelVersion, rModelName);
        BlobLocation location = resolveBlobLocation(manager, rResourceGroupName, rWorkspaceName, modelUri);
        BlobContainerClient container = MachineLearningService.blobContainerClient(credentials(runContext), location.accountName(), location.containerName());

        // Check for files under the folder prefix FIRST: a folder-based model can carry a zero-byte placeholder
        // blob named exactly after its own folder path (left behind by tools like Storage Explorer/azcopy), which
        // would otherwise be indistinguishable from a genuine single-file model at that same path.
        String prefix = location.blobPath().endsWith("/") ? location.blobPath() : location.blobPath() + "/";

        URI resultUri;
        boolean archive;
        try {
            List<BlobItem> folderFiles = container.listBlobs(new ListBlobsOptions().setPrefix(prefix), Duration.ofSeconds(30)).stream()
                .filter(file -> !file.getName().equals(prefix))
                .toList();

            if (!folderFiles.isEmpty()) {
                archive = true;
                File archiveFile = runContext.workingDir().createTempFile(".zip").toFile();
                try (var zip = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(archiveFile)))) {
                    for (BlobItem file : folderFiles) {
                        zip.putNextEntry(new ZipEntry(file.getName().substring(prefix.length())));
                        container.getBlobClient(file.getName()).downloadStream(zip);
                        zip.closeEntry();
                    }
                }
                resultUri = runContext.storage().putFile(archiveFile);
            } else if (container.getBlobClient(location.blobPath()).exists()) {
                archive = false;
                File tempFile = runContext.workingDir().createTempFile().toFile();
                container.getBlobClient(location.blobPath()).downloadToFile(tempFile.getAbsolutePath(), true);
                resultUri = runContext.storage().putFile(tempFile);
            } else {
                throw new IllegalStateException("No files found for model '%s' version '%s' at '%s'".formatted(rModelName, modelVersion.name(), modelUri));
            }
        } catch (BlobStorageException e) {
            throw new IllegalStateException(
                "Failed to download model '%s' version '%s' from blob storage — check the workspace's managed identity/service principal has read access to the backing storage account: %s".formatted(rModelName, modelVersion.name(), e.getMessage()), e
            );
        }

        logger.info("Downloaded model '{}' version '{}' to internal storage", rModelName, modelVersion.name());

        return Output.builder()
            .modelName(rModelName)
            .version(modelVersion.name())
            .uri(resultUri)
            .archive(archive)
            .build();
    }

    private BlobLocation resolveBlobLocation(MachineLearningManager manager, String resourceGroupName, String workspaceName, String modelUri) {
        var datastorePath = MachineLearningService.parseDatastoreUri(modelUri);
        if (datastorePath != null) {
            var datastore = manager.datastores().get(resourceGroupName, workspaceName, datastorePath.datastoreName());
            if (!(datastore.properties() instanceof AzureBlobDatastore blobDatastore)) {
                throw new IllegalStateException(
                    "Datastore '%s' backing this model is not an Azure Blob datastore, downloading is not supported for this datastore type".formatted(datastorePath.datastoreName())
                );
            }
            return new BlobLocation(blobDatastore.accountName(), blobDatastore.containerName(), datastorePath.path());
        }

        URI uri = URI.create(modelUri);
        if (uri.getHost() == null) {
            throw new IllegalArgumentException("Unsupported model URI '%s': expected an `azureml://datastores/.../paths/...` or a blob container URI with a host and a path".formatted(modelUri));
        }
        String accountName = uri.getHost().split("\\.", 2)[0];
        String[] pathParts = uri.getPath().replaceFirst("^/", "").split("/", 2);
        if (pathParts.length < 2) {
            throw new IllegalArgumentException("Unsupported model URI '%s': expected an `azureml://datastores/.../paths/...` or a blob container URI with a path".formatted(modelUri));
        }
        return new BlobLocation(accountName, pathParts[0], pathParts[1]);
    }

    private record BlobLocation(String accountName, String containerName, String blobPath) {
    }

    @SuperBuilder
    @Getter
    @NoArgsConstructor
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Model name")
        private String modelName;

        @Schema(title = "Model version", description = "Resolved version, useful when `version=latest` was requested")
        private String version;

        @Schema(title = "Internal storage URI", description = "`kestra://` URI of the downloaded model artifact")
        private URI uri;

        @Schema(title = "Archive", description = "True when the model had multiple files and was packaged into a single ZIP archive")
        private Boolean archive;
    }
}
