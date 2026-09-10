package io.kestra.plugin.azure.ml;

import java.net.URI;
import java.util.Optional;

import com.azure.core.management.exception.ManagementException;
import com.azure.resourcemanager.machinelearning.MachineLearningManager;
import com.azure.resourcemanager.machinelearning.models.DataContainer;
import com.azure.resourcemanager.machinelearning.models.DataContainerProperties;
import com.azure.resourcemanager.machinelearning.models.DataType;
import com.azure.resourcemanager.machinelearning.models.DataVersionBase;
import com.azure.resourcemanager.machinelearning.models.DataVersionBaseProperties;
import com.azure.resourcemanager.machinelearning.models.MLTableData;
import com.azure.resourcemanager.machinelearning.models.UriFileDataVersion;
import com.azure.resourcemanager.machinelearning.models.UriFolderDataVersion;

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
                id: azure_ml_create_data_asset
                namespace: company.team

                tasks:
                  - id: register_dataset
                    type: io.kestra.plugin.azure.ml.CreateDataAsset
                    tenantId: "{{ secret('AZURE_TENANT_ID') }}"
                    clientId: "{{ secret('AZURE_CLIENT_ID') }}"
                    clientSecret: "{{ secret('AZURE_CLIENT_SECRET') }}"
                    subscriptionId: "{{ secret('AZURE_SUBSCRIPTION_ID') }}"
                    resourceGroupName: ml-rg
                    workspaceName: ml-workspace
                    dataName: training-dataset
                    dataAssetType: URI_FOLDER
                    uri: "azureml://datastores/workspaceblobstore/paths/data/training"
                """
        )
    }
)
@Schema(
    title = "Register a data asset version in Azure Machine Learning",
    description = "Creates a new version of a data asset (`URI_FILE`, `URI_FOLDER` or `MLTABLE`) pointing to an existing datastore path. Data asset versions are immutable: set `dataVersion` explicitly to control it, or leave it empty to auto-increment from the asset's latest version."
)
public class CreateDataAsset extends AbstractMachineLearningTask implements RunnableTask<CreateDataAsset.Output> {
    @Schema(title = "Data asset name")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> dataName;

    @Schema(title = "Data asset version", description = "Explicit version to register; when not set, the next version is computed automatically from the asset's current latest version")
    @PluginProperty(group = "main")
    private Property<String> dataVersion;

    @Schema(title = "Data asset type", description = "Kind of data referenced by `uri`")
    @NotNull
    @PluginProperty(group = "main")
    private Property<DataAssetType> dataAssetType;

    @Schema(title = "Data URI", description = "Storage URI for the data, e.g. `azureml://datastores/<name>/paths/<path>`")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> uri;

    @Schema(title = "Description", description = "Free-text description stored with the data asset version")
    @PluginProperty(group = "advanced")
    private Property<String> dataDescription;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var logger = runContext.logger();
        String rResourceGroupName = rResourceGroupName(runContext);
        String rWorkspaceName = rWorkspaceName(runContext);
        String rDataName = runContext.render(this.dataName).as(String.class).orElseThrow();
        DataAssetType rDataAssetType = runContext.render(this.dataAssetType).as(DataAssetType.class).orElseThrow();
        String rUri = runContext.render(this.uri).as(String.class).orElseThrow();

        MachineLearningManager manager = machineLearningManager(runContext);

        DataContainer container = getOrCreateDataContainer(manager, rResourceGroupName, rWorkspaceName, rDataName, rDataAssetType);
        Optional<String> explicitVersion = runContext.render(this.dataVersion).as(String.class).filter(v -> !v.isBlank());

        DataVersionBaseProperties properties = switch (rDataAssetType) {
            case URI_FILE -> new UriFileDataVersion().withDataUri(rUri);
            case URI_FOLDER -> new UriFolderDataVersion().withDataUri(rUri);
            case MLTABLE -> new MLTableData().withDataUri(rUri);
        };
        runContext.render(this.dataDescription).as(String.class).ifPresent(properties::withDescription);

        String rVersion = explicitVersion.orElseGet(() -> MachineLearningService.requireNextVersion(container, rDataName));
        DataVersionBase createdDataVersion = null;
        for (int attempt = 0; createdDataVersion == null; attempt++) {
            try {
                createdDataVersion = manager.dataVersions()
                    .define(rVersion)
                    .withExistingData(rResourceGroupName, rWorkspaceName, rDataName)
                    .withProperties(properties)
                    .create();
            } catch (ManagementException e) {
                if (e.getResponse() == null || e.getResponse().getStatusCode() != 409) {
                    throw e;
                }
                if (explicitVersion.isPresent() || attempt >= 4) {
                    throw new IllegalArgumentException(
                        "Data asset '%s' version '%s' already exists — data asset versions are immutable, set a different `dataVersion` or omit it to auto-increment"
                            .formatted(rDataName, rVersion),
                        e
                    );
                }
                // Auto-incremented version raced with a concurrent registration; re-read the container's next
                // version and retry, instead of failing on a version number that is already known to be stale.
                rVersion = MachineLearningService.requireNextVersion(manager.dataContainers().get(rResourceGroupName, rWorkspaceName, rDataName), rDataName);
            }
        }

        logger.info("Registered data asset '{}' version '{}' from '{}'", rDataName, rVersion, rUri);

        return Output.builder()
            .dataName(rDataName)
            .version(createdDataVersion.name())
            .uri(URI.create(rUri))
            .build();
    }

    private static DataContainer getOrCreateDataContainer(MachineLearningManager manager, String resourceGroupName, String workspaceName, String dataName, DataAssetType dataAssetType) {
        try {
            return manager.dataContainers().get(resourceGroupName, workspaceName, dataName);
        } catch (ManagementException e) {
            if (e.getResponse() == null || e.getResponse().getStatusCode() != 404) {
                throw e;
            }
        }
        try {
            return manager.dataContainers()
                .define(dataName)
                .withExistingWorkspace(resourceGroupName, workspaceName)
                .withProperties(new DataContainerProperties().withDataType(DataType.fromString(dataAssetType.wireValue())))
                .create();
        } catch (ManagementException e) {
            if (e.getResponse() != null && e.getResponse().getStatusCode() == 409) {
                // A concurrent execution created the container between our get() and this create() — it exists
                // now, which is exactly what this method is asked to return.
                return manager.dataContainers().get(resourceGroupName, workspaceName, dataName);
            }
            throw e;
        }
    }

    @SuperBuilder
    @Getter
    @NoArgsConstructor
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Data asset name")
        private String dataName;

        @Schema(title = "Data asset version", description = "Version registered, either the explicit `version` or the auto-incremented one")
        private String version;

        @Schema(title = "Data URI", description = "Storage URI backing this data asset version")
        private URI uri;
    }
}
