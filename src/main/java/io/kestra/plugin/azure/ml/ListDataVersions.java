package io.kestra.plugin.azure.ml;

import java.net.URI;
import java.util.Comparator;
import java.util.List;

import com.azure.core.management.exception.ManagementException;
import com.azure.resourcemanager.machinelearning.MachineLearningManager;

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
                id: azure_ml_list_data_versions
                namespace: company.team

                tasks:
                  - id: list_versions
                    type: io.kestra.plugin.azure.ml.ListDataVersions
                    tenantId: "{{ secret('AZURE_TENANT_ID') }}"
                    clientId: "{{ secret('AZURE_CLIENT_ID') }}"
                    clientSecret: "{{ secret('AZURE_CLIENT_SECRET') }}"
                    subscriptionId: "{{ secret('AZURE_SUBSCRIPTION_ID') }}"
                    resourceGroupName: ml-rg
                    workspaceName: ml-workspace
                    dataName: training-dataset
                """
        )
    }
)
@Schema(
    title = "List the versions of an Azure Machine Learning data asset",
    description = "Lists every registered version of a data asset, most recent first."
)
public class ListDataVersions extends AbstractMachineLearningTask implements RunnableTask<ListDataVersions.Output> {
    @Schema(title = "Data asset name")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> dataName;

    @Override
    public Output run(RunContext runContext) throws Exception {
        String rResourceGroupName = rResourceGroupName(runContext);
        String rWorkspaceName = rWorkspaceName(runContext);
        String rDataName = runContext.render(this.dataName).as(String.class).orElseThrow();

        MachineLearningManager manager = machineLearningManager(runContext);

        List<Version> versions;
        try {
            versions = manager.dataVersions().list(rResourceGroupName, rWorkspaceName, rDataName).stream()
                .map(
                    dataVersion -> Version.builder()
                        .version(dataVersion.name())
                        .uri(URI.create(dataVersion.properties().dataUri()))
                        .build()
                )
                .sorted(Comparator.comparing(Version::getVersion, MachineLearningService.VERSION_COMPARATOR).reversed())
                .toList();
        } catch (ManagementException e) {
            if (e.getResponse() != null && e.getResponse().getStatusCode() == 404) {
                throw new IllegalArgumentException("Data asset '%s' was not found in workspace '%s'".formatted(rDataName, rWorkspaceName), e);
            }
            throw e;
        }

        return Output.builder()
            .dataName(rDataName)
            .versions(versions)
            .build();
    }

    @lombok.Builder
    @Getter
    public static class Version {
        @Schema(title = "Data asset version")
        private String version;

        @Schema(title = "Data URI", description = "Storage URI backing this data asset version")
        private URI uri;
    }

    @SuperBuilder
    @Getter
    @NoArgsConstructor
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Data asset name")
        private String dataName;

        @Schema(title = "Versions", description = "Every registered version of the data asset, most recent first")
        private List<Version> versions;
    }
}
