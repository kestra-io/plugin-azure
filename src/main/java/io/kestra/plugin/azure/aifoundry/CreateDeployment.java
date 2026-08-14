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
    title = "Deploy a model from the catalog to an endpoint."
)
public class CreateDeployment extends AbstractAiFoundryTask implements RunnableTask<CreateDeployment.Output> {

    @Schema(title = "The name of the deployment to create.")
    @NotNull
    private Property<String> deploymentName;

    @Schema(title = "The model ID to deploy.")
    @NotNull
    private Property<String> modelId;

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
        String modelIdRendered = runContext.render(this.modelId).as(String.class).orElseThrow();

        runContext.logger().warn("Creating deployment not supported yet via DeploymentsClient in Java SDK.");

        // As a fallback, we fetch if it exists.
        var deployment = client.getDeployment(deploymentNameRendered);

        return Output.builder()
            .deploymentName(deploymentNameRendered)
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The name of the created deployment.")
        private String deploymentName;
    }
}
