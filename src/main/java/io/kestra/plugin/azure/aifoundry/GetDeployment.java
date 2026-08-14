package io.kestra.plugin.azure.aifoundry;

import com.azure.ai.projects.AIProjectClientBuilder;
import com.azure.core.credential.KeyCredential;
import com.azure.core.credential.TokenCredential;

import io.kestra.core.models.annotations.Plugin;
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
@Plugin
@Schema(
    title = "Retrieve deployment status and configuration from Azure AI Foundry."
)
public class GetDeployment extends AbstractAiFoundryTask implements RunnableTask<GetDeployment.Output> {

    @Schema(title = "The name of the deployment to retrieve.")
    @NotNull
    private Property<String> deploymentName;

    @Override
    public Output run(RunContext runContext) throws Exception {
        AIProjectClientBuilder builder = new AIProjectClientBuilder()
            .endpoint(this.getEndpoint(runContext));

        KeyCredential key = this.getKeyCredential(runContext);
        if (key != null) {
            throw new IllegalArgumentException("Deployments client only supports Entra ID. Do not provide apiKey.");
        } else {
            TokenCredential token = this.getTokenCredential(runContext);
            builder.credential(token);
        }

        var client = builder.buildDeploymentsClient();
        String deploymentNameRendered = runContext.render(this.deploymentName).as(String.class).orElseThrow();

        var deployment = client.getDeployment(deploymentNameRendered);

        runContext.logger().info("Retrieved deployment {}", deployment.getName());

        return Output.builder()
            .name(deployment.getName())
            .deployment(deployment)
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Deployment name.")
        private String name;

        @Schema(title = "Deployment configuration.")
        private Object configuration;
    }
}
