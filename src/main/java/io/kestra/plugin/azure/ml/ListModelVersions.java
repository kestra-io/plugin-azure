package io.kestra.plugin.azure.ml;

import java.net.URI;
import java.util.Comparator;
import java.util.List;

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
            full = true,
            code = """
                id: azure_ml_list_model_versions
                namespace: company.team

                tasks:
                  - id: list_versions
                    type: io.kestra.plugin.azure.ml.ListModelVersions
                    tenantId: "{{ secret('AZURE_TENANT_ID') }}"
                    clientId: "{{ secret('AZURE_CLIENT_ID') }}"
                    clientSecret: "{{ secret('AZURE_CLIENT_SECRET') }}"
                    subscriptionId: "{{ secret('AZURE_SUBSCRIPTION_ID') }}"
                    resourceGroupName: ml-rg
                    workspaceName: ml-workspace
                    modelName: fraud-detector
                """
        )
    }
)
@Schema(
    title = "List the versions of an Azure Machine Learning model",
    description = "Lists every registered version of a model, most recent first."
)
public class ListModelVersions extends AbstractMachineLearningTask implements RunnableTask<ListModelVersions.Output> {
    @Schema(title = "Model name")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> modelName;

    @Override
    public Output run(RunContext runContext) throws Exception {
        String rResourceGroupName = rResourceGroupName(runContext);
        String rWorkspaceName = rWorkspaceName(runContext);
        String rModelName = runContext.render(this.modelName).as(String.class).orElseThrow();

        MachineLearningManager manager = machineLearningManager(runContext);

        List<Version> versions;
        try {
            versions = manager.modelVersions().list(rResourceGroupName, rWorkspaceName, rModelName).stream()
                .sorted(Comparator.comparing((ModelVersion v) -> v.systemData() != null ? v.systemData().createdAt() : null, Comparator.nullsFirst(Comparator.naturalOrder())).reversed())
                .map(
                    modelVersion -> Version.builder()
                        .version(modelVersion.name())
                        .modelUri(URI.create(modelVersion.properties().modelUri()))
                        .modelType(modelVersion.properties().modelType())
                        .build()
                )
                .toList();
        } catch (ManagementException e) {
            if (e.getResponse() != null && e.getResponse().getStatusCode() == 404) {
                throw new IllegalArgumentException("Model '%s' was not found in workspace '%s'".formatted(rModelName, rWorkspaceName), e);
            }
            throw e;
        }

        return Output.builder()
            .modelName(rModelName)
            .versions(versions)
            .build();
    }

    @lombok.Builder
    @Getter
    public static class Version {
        @Schema(title = "Model version")
        private String version;

        @Schema(title = "Model URI", description = "Storage URI backing this model version")
        private URI modelUri;

        @Schema(title = "Model type", description = "Model framework flavor, e.g. `custom_model`, `mlflow_model`")
        private String modelType;
    }

    @SuperBuilder
    @Getter
    @NoArgsConstructor
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Model name")
        private String modelName;

        @Schema(title = "Versions", description = "Every registered version of the model, most recent first")
        private List<Version> versions;
    }
}
