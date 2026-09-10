package io.kestra.plugin.azure.ml;

import java.net.URI;

import com.azure.core.management.exception.ManagementException;
import com.azure.resourcemanager.machinelearning.MachineLearningManager;
import com.azure.resourcemanager.machinelearning.models.JobBase;
import com.azure.resourcemanager.machinelearning.models.ModelContainer;
import com.azure.resourcemanager.machinelearning.models.ModelContainerProperties;
import com.azure.resourcemanager.machinelearning.models.ModelVersion;
import com.azure.resourcemanager.machinelearning.models.ModelVersionProperties;

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
            title = "Register a model produced by a training job.",
            full = true,
            code = """
                id: azure_ml_register_model_from_job
                namespace: company.team

                tasks:
                  - id: train
                    type: io.kestra.plugin.azure.ml.SubmitCommandJob
                    tenantId: "{{ secret('AZURE_TENANT_ID') }}"
                    clientId: "{{ secret('AZURE_CLIENT_ID') }}"
                    clientSecret: "{{ secret('AZURE_CLIENT_SECRET') }}"
                    subscriptionId: "{{ secret('AZURE_SUBSCRIPTION_ID') }}"
                    resourceGroupName: ml-rg
                    workspaceName: ml-workspace
                    computeName: cpu-cluster
                    environmentId: "azureml:AzureML-sklearn-1.5:1"
                    command: "python train.py"
                    outputs:
                      model_dir: "azureml://datastores/workspaceblobstore/paths/outputs/model"

                  - id: register
                    type: io.kestra.plugin.azure.ml.RegisterModel
                    tenantId: "{{ secret('AZURE_TENANT_ID') }}"
                    clientId: "{{ secret('AZURE_CLIENT_ID') }}"
                    clientSecret: "{{ secret('AZURE_CLIENT_SECRET') }}"
                    subscriptionId: "{{ secret('AZURE_SUBSCRIPTION_ID') }}"
                    resourceGroupName: ml-rg
                    workspaceName: ml-workspace
                    modelName: fraud-detector
                    source: JOB_OUTPUT
                    jobName: "{{ outputs.train.jobName }}"
                    jobOutputName: model_dir
                """
        ),
        @Example(
            title = "Register a model file stored in Kestra's internal storage.",
            full = true,
            code = """
                id: azure_ml_register_model_from_storage
                namespace: company.team

                inputs:
                  - id: model_file
                    type: FILE

                tasks:
                  - id: register
                    type: io.kestra.plugin.azure.ml.RegisterModel
                    tenantId: "{{ secret('AZURE_TENANT_ID') }}"
                    clientId: "{{ secret('AZURE_CLIENT_ID') }}"
                    clientSecret: "{{ secret('AZURE_CLIENT_SECRET') }}"
                    subscriptionId: "{{ secret('AZURE_SUBSCRIPTION_ID') }}"
                    resourceGroupName: ml-rg
                    workspaceName: ml-workspace
                    modelName: fraud-detector
                    source: INTERNAL_STORAGE
                    from: "{{ inputs.model_file }}"
                """
        )
    }
)
@Schema(
    title = "Register a model version in the Azure Machine Learning model registry",
    description = "Creates a new version of a model, capturing lineage back to the training job (`JOB_OUTPUT`), a file uploaded to the workspace's default datastore (`INTERNAL_STORAGE`), or an existing datastore path (`DATASTORE_URI`). Model versions are immutable: set `modelVersion` explicitly to control it, or leave it empty to auto-increment from the model's latest version."
)
public class RegisterModel extends AbstractMachineLearningTask implements RunnableTask<RegisterModel.Output> {
    @Schema(title = "Model name", description = "Name of the model to register a new version for")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> modelName;

    @Schema(title = "Model version", description = "Explicit version to register; when not set, the next version is computed automatically from the model's current latest version")
    @PluginProperty(group = "main")
    private Property<String> modelVersion;

    @Schema(title = "Model source", description = "Origin of the model artifact being registered")
    @NotNull
    @PluginProperty(group = "main")
    private Property<ModelSource> source;

    @Schema(title = "Job name", description = "Required when `source=JOB_OUTPUT`: the Azure Machine Learning job that produced the model artifact")
    @PluginProperty(group = "source")
    private Property<String> jobName;

    @Schema(title = "Job output name", description = "Required when `source=JOB_OUTPUT`: the named job output pointing to the model artifact")
    @PluginProperty(group = "source")
    private Property<String> jobOutputName;

    @Schema(title = "Internal storage file", description = "Required when `source=INTERNAL_STORAGE`: a `kestra://` internal storage URI for the model artifact to upload")
    @PluginProperty(group = "source", internalStorageURI = true)
    private Property<String> from;

    @Schema(title = "Datastore URI", description = "Required when `source=DATASTORE_URI`: an existing `azureml://datastores/<name>/paths/<path>` (or plain storage) URI for the model artifact")
    @PluginProperty(group = "source")
    private Property<String> datastoreUri;

    @Schema(title = "Model type", description = "Model framework flavor, e.g. `custom_model`, `mlflow_model`, `triton_model`; defaults to `custom_model`")
    @lombok.Builder.Default
    @PluginProperty(group = "advanced")
    private Property<String> modelType = Property.ofValue("custom_model");

    @Schema(title = "Description", description = "Free-text description stored with the model version")
    @PluginProperty(group = "advanced")
    private Property<String> modelDescription;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var logger = runContext.logger();
        String rResourceGroupName = rResourceGroupName(runContext);
        String rWorkspaceName = rWorkspaceName(runContext);
        String rModelName = runContext.render(this.modelName).as(String.class).orElseThrow();
        ModelSource rSource = runContext.render(this.source).as(ModelSource.class).orElseThrow();

        MachineLearningManager manager = machineLearningManager(runContext);

        String modelUri = switch (rSource) {
            case JOB_OUTPUT -> resolveFromJobOutput(runContext, manager, rResourceGroupName, rWorkspaceName);
            case INTERNAL_STORAGE -> resolveFromInternalStorage(runContext, manager, rResourceGroupName, rWorkspaceName, rModelName);
            case DATASTORE_URI -> runContext.render(this.datastoreUri).as(String.class)
                .orElseThrow(() -> new IllegalArgumentException("`datastoreUri` is required when `source=DATASTORE_URI`"));
        };

        ModelContainer container = getOrCreateModelContainer(manager, rResourceGroupName, rWorkspaceName, rModelName);
        String rVersion = runContext.render(this.modelVersion).as(String.class).orElseGet(() -> container.properties().nextVersion());

        ModelVersionProperties properties = new ModelVersionProperties()
            .withModelUri(modelUri)
            .withModelType(runContext.render(this.modelType).as(String.class).orElse("custom_model"));
        runContext.render(this.modelDescription).as(String.class).ifPresent(properties::withDescription);

        ModelVersion createdModelVersion;
        try {
            createdModelVersion = manager.modelVersions()
                .define(rVersion)
                .withExistingModel(rResourceGroupName, rWorkspaceName, rModelName)
                .withProperties(properties)
                .create();
        } catch (ManagementException e) {
            if (e.getResponse() != null && e.getResponse().getStatusCode() == 409) {
                throw new IllegalArgumentException(
                    "Model '%s' version '%s' already exists — model versions are immutable, set a different `modelVersion` or omit it to auto-increment".formatted(rModelName, rVersion), e
                );
            }
            throw e;
        }

        logger.info("Registered model '{}' version '{}' from '{}'", rModelName, rVersion, modelUri);

        return Output.builder()
            .modelName(rModelName)
            .version(rVersion)
            .modelUri(URI.create(modelUri))
            .studioUrl(
                "https://ml.azure.com/model/%s:%s/details?wsid=/subscriptions/%s/resourcegroups/%s/workspaces/%s"
                    .formatted(rModelName, rVersion, rSubscriptionId(runContext), rResourceGroupName, rWorkspaceName)
            )
            .build();
    }

    private String resolveFromJobOutput(RunContext runContext, MachineLearningManager manager, String resourceGroupName, String workspaceName) throws Exception {
        String rJobName = runContext.render(this.jobName).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("`jobName` is required when `source=JOB_OUTPUT`"));
        String rJobOutputName = runContext.render(this.jobOutputName).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("`jobOutputName` is required when `source=JOB_OUTPUT`"));

        JobBase job;
        try {
            job = manager.jobs().get(resourceGroupName, workspaceName, rJobName);
        } catch (ManagementException e) {
            if (e.getResponse() != null && e.getResponse().getStatusCode() == 404) {
                throw new IllegalArgumentException("Job '%s' was not found in workspace '%s'".formatted(rJobName, workspaceName), e);
            }
            throw e;
        }

        var outputs = MachineLearningService.namedOutputs(MachineLearningService.jobOutputs(job));
        URI output = outputs.get(rJobOutputName);
        if (output == null) {
            throw new IllegalArgumentException("Job '%s' has no output named '%s' — available outputs: %s".formatted(rJobName, rJobOutputName, outputs.keySet()));
        }
        return output.toString();
    }

    private String resolveFromInternalStorage(RunContext runContext, MachineLearningManager manager, String resourceGroupName, String workspaceName, String modelName) throws Exception {
        String rFrom = runContext.render(this.from).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("`from` is required when `source=INTERNAL_STORAGE`"));
        URI internalStorageUri = URI.create(rFrom);
        String fileName = internalStorageUri.getPath().substring(internalStorageUri.getPath().lastIndexOf('/') + 1);
        String destinationPath = "kestra/models/%s/%s".formatted(modelName, fileName);

        return MachineLearningService.uploadToDefaultDatastore(
            runContext,
            manager,
            credentials(runContext),
            resourceGroupName,
            workspaceName,
            internalStorageUri,
            destinationPath
        );
    }

    private static ModelContainer getOrCreateModelContainer(MachineLearningManager manager, String resourceGroupName, String workspaceName, String modelName) {
        try {
            return manager.modelContainers().get(resourceGroupName, workspaceName, modelName);
        } catch (ManagementException e) {
            if (e.getResponse() == null || e.getResponse().getStatusCode() != 404) {
                throw e;
            }
        }
        return manager.modelContainers()
            .define(modelName)
            .withExistingWorkspace(resourceGroupName, workspaceName)
            .withProperties(new ModelContainerProperties())
            .create();
    }

    @SuperBuilder
    @Getter
    @NoArgsConstructor
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Model name")
        private String modelName;

        @Schema(title = "Model version", description = "Version registered, either the explicit `version` or the auto-incremented one")
        private String version;

        @Schema(title = "Model URI", description = "Storage URI backing this model version")
        private URI modelUri;

        @Schema(title = "Studio URL", description = "Deep link to the model version in Azure ML Studio")
        private String studioUrl;
    }
}
