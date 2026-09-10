package io.kestra.plugin.azure.ml;

import java.net.URI;

import com.azure.core.management.exception.ManagementException;
import com.azure.resourcemanager.machinelearning.MachineLearningManager;
import com.azure.resourcemanager.machinelearning.models.ModelVersion;

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
            title = "Get the latest version of a registered model.",
            full = true,
            code = """
                id: azure_ml_get_model
                namespace: company.team

                tasks:
                  - id: get_model
                    type: io.kestra.plugin.azure.ml.GetModel
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
    title = "Get an Azure Machine Learning model version",
    description = "Reads the metadata of a specific model version, or the latest one when `modelVersion=latest`."
)
public class GetModel extends AbstractMachineLearningTask implements RunnableTask<GetModel.Output> {
    @Schema(title = "Model name")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> modelName;

    @Schema(title = "Model version", description = "Specific version to fetch, or `latest` (default) to resolve the most recent one")
    @lombok.Builder.Default
    @PluginProperty(group = "main")
    private Property<String> modelVersion = Property.ofValue("latest");

    @Override
    public Output run(RunContext runContext) throws Exception {
        String rResourceGroupName = rResourceGroupName(runContext);
        String rWorkspaceName = rWorkspaceName(runContext);
        String rModelName = runContext.render(this.modelName).as(String.class).orElseThrow();
        String rVersion = runContext.render(this.modelVersion).as(String.class).orElse("latest");

        MachineLearningManager manager = machineLearningManager(runContext);

        ModelVersion modelVersion;
        try {
            if ("latest".equalsIgnoreCase(rVersion)) {
                modelVersion = MachineLearningService.latestModelVersion(manager, rResourceGroupName, rWorkspaceName, rModelName);
            } else {
                modelVersion = manager.modelVersions().get(rResourceGroupName, rWorkspaceName, rModelName, rVersion);
            }
        } catch (ManagementException e) {
            if (e.getResponse() != null && e.getResponse().getStatusCode() == 404) {
                throw new IllegalArgumentException("Model '%s' version '%s' was not found in workspace '%s'".formatted(rModelName, rVersion, rWorkspaceName), e);
            }
            throw e;
        }

        if (modelVersion == null) {
            throw new IllegalArgumentException("Model '%s' has no registered version in workspace '%s'".formatted(rModelName, rWorkspaceName));
        }

        return Output.builder()
            .modelName(rModelName)
            .version(modelVersion.name())
            .modelUri(URI.create(modelVersion.properties().modelUri()))
            .modelType(modelVersion.properties().modelType())
            .build();
    }

    @SuperBuilder
    @Getter
    @NoArgsConstructor
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Model name")
        private String modelName;

        @Schema(title = "Model version", description = "Resolved version, useful when `version=latest` was requested")
        private String version;

        @Schema(title = "Model URI", description = "Storage URI backing this model version")
        private URI modelUri;

        @Schema(title = "Model type", description = "Model framework flavor, e.g. `custom_model`, `mlflow_model`")
        private String modelType;
    }
}
