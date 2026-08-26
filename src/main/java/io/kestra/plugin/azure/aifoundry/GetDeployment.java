package io.kestra.plugin.azure.aifoundry;

import com.azure.ai.projects.AIProjectClientBuilder;
import com.azure.ai.projects.DeploymentsClient;
import com.azure.ai.projects.models.Deployment;
import com.azure.ai.projects.models.ModelDeployment;
import com.azure.core.credential.TokenCredential;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
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
            title = "Retrieve a model deployment from Azure AI Foundry",
            code = """
                    id: azure_ai_get_deployment
                    namespace: company.team
                    tasks:
                      - id: get_deployment
                        type: io.kestra.plugin.azure.aifoundry.GetDeployment
                        endpoint: "{{ secret('AZURE_AI_FOUNDRY_ENDPOINT') }}"
                        deploymentName: gpt-4o
                """
        )
    }
)
@Schema(
    title = "Retrieve deployment status and configuration from Azure AI Foundry",
    description = "Fetches a named deployment via the Azure AI Projects DeploymentsClient. " +
        "Requires Entra ID authentication (DefaultAzureCredential); API-key authentication is not supported by this client."
)
public class GetDeployment extends AbstractAiFoundryTask implements RunnableTask<GetDeployment.Output> {

    @Schema(title = "The name of the deployment to retrieve")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> deploymentName;

    @Override
    public Output run(RunContext runContext) throws Exception {
        if (this.getKeyCredential(runContext) != null) {
            throw new IllegalArgumentException(
                "GetDeployment uses the Azure AI Projects DeploymentsClient which only supports " +
                    "Entra ID authentication. Remove the apiKey property and configure DefaultAzureCredential " +
                    "(e.g. via AZURE_CLIENT_ID / AZURE_CLIENT_SECRET / AZURE_TENANT_ID environment variables)."
            );
        }

        TokenCredential token = this.getTokenCredential(runContext);
        DeploymentsClient client = new AIProjectClientBuilder()
            .endpoint(this.getEndpoint(runContext))
            .credential(token)
            .buildDeploymentsClient();

        String deploymentNameRendered = runContext.render(this.deploymentName)
            .as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("deploymentName is required"));

        Deployment deployment = client.getDeployment(deploymentNameRendered);

        runContext.logger().info("Retrieved deployment {} (type: {})", deployment.getName(), deployment.getType());

        DeploymentRecord.DeploymentRecordBuilder recordBuilder = DeploymentRecord.builder()
            .name(deployment.getName())
            .type(deployment.getType() != null ? deployment.getType().toString() : null);

        if (deployment instanceof ModelDeployment modelDeployment) {
            recordBuilder
                .modelName(modelDeployment.getModelName())
                .modelVersion(modelDeployment.getModelVersion())
                .modelPublisher(modelDeployment.getModelPublisher())
                .connectionName(modelDeployment.getConnectionName());
        }

        return Output.builder()
            .name(deployment.getName())
            .type(deployment.getType() != null ? deployment.getType().toString() : null)
            .configuration(recordBuilder.build())
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Deployment name")
        private String name;

        @Schema(title = "Deployment type")
        private String type;

        @Schema(title = "Full deployment configuration object")
        private DeploymentRecord configuration;
    }

    @Builder
    @Getter
    public static class DeploymentRecord {
        private String name;
        private String type;
        private String modelName;
        private String modelVersion;
        private String modelPublisher;
        private String connectionName;
    }
}
