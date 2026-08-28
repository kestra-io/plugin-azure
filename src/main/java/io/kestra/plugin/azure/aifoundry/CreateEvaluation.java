package io.kestra.plugin.azure.aifoundry;

import java.util.HashMap;
import java.util.Map;

import com.azure.ai.projects.AIProjectClientBuilder;
import com.azure.ai.projects.EvaluationsClient;
import com.azure.ai.projects.models.Evaluation;
import com.azure.ai.projects.models.EvaluatorConfiguration;
import com.azure.ai.projects.models.InputDataset;
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
            title = "Create a new evaluation run in Azure AI Foundry",
            code = """
                    id: azure_ai_create_evaluation
                    namespace: company.team
                    tasks:
                      - id: create_eval
                        type: io.kestra.plugin.azure.aifoundry.CreateEvaluation
                        endpoint: "{{ secret('AZURE_AI_FOUNDRY_ENDPOINT') }}"
                        tenantId: "{{ secret('AZURE_TENANT_ID') }}"
                        clientId: "{{ secret('AZURE_CLIENT_ID') }}"
                        clientSecret: "{{ secret('AZURE_CLIENT_SECRET') }}"
                        datasetId: "azureml:my-dataset:1"
                        displayName: "Nightly Groundedness Eval"
                        evaluators:
                          groundedness: "azureml://registries/azureml/models/Groundedness-Evaluator/versions/1"
                """
        )
    }
)
@Schema(
    title = "Create an evaluation run in Azure AI Foundry",
    description = "Submits a new evaluation job using a dataset and a set of evaluators. " +
        "Requires Entra ID authentication (DefaultAzureCredential)."
)
public class CreateEvaluation extends AbstractAiFoundryTask implements RunnableTask<CreateEvaluation.Output> {

    @Schema(
        title = "Dataset ID",
        description = "The Azure AI dataset ID to evaluate (e.g. azureml:my-dataset:1)."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> datasetId;

    @Schema(
        title = "Display Name",
        description = "Human-readable label for this evaluation run."
    )
    @PluginProperty(group = "main")
    private Property<String> displayName;

    @Schema(
        title = "Evaluators",
        description = "A map of evaluator keys to their Azure ML evaluator IDs."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<Map<String, String>> evaluators;

    @Override
    public Output run(RunContext runContext) throws Exception {
        if (this.getKeyCredential(runContext) != null) {
            throw new IllegalArgumentException(
                "CreateEvaluation uses the Azure AI Projects EvaluationsClient which only supports " +
                    "Entra ID authentication. Remove the apiKey property and configure DefaultAzureCredential."
            );
        }

        TokenCredential token = this.getTokenCredential(runContext);
        EvaluationsClient client = new AIProjectClientBuilder()
            .endpoint(this.getEndpoint(runContext))
            .credential(token)
            .buildEvaluationsClient();

        String datasetIdRendered = runContext.render(this.datasetId)
            .as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("datasetId is required"));

        Map<String, String> evaluatorsRendered = runContext.render(this.evaluators).asMap(String.class, String.class);
        if (evaluatorsRendered.isEmpty()) {
            throw new IllegalArgumentException("evaluators map cannot be empty");
        }

        Map<String, EvaluatorConfiguration> evaluatorConfigs = new HashMap<>();
        for (Map.Entry<String, String> entry : evaluatorsRendered.entrySet()) {
            evaluatorConfigs.put(entry.getKey(), new EvaluatorConfiguration(entry.getValue()));
        }

        InputDataset inputDataset = new InputDataset(datasetIdRendered);
        Evaluation evaluation = new Evaluation(inputDataset, evaluatorConfigs);

        String displayNameRendered = runContext.render(this.displayName).as(String.class).orElse(null);
        if (displayNameRendered != null) {
            evaluation.setDisplayName(displayNameRendered);
        }

        Evaluation createdEvaluation = client.createEvaluation(evaluation);

        runContext.logger().info("Created evaluation {} (status: {})", createdEvaluation.getName(), createdEvaluation.getStatus());

        return Output.builder()
            .name(createdEvaluation.getName())
            .status(createdEvaluation.getStatus())
            .displayName(createdEvaluation.getDisplayName())
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The assigned evaluation name / ID")
        private String name;

        @Schema(title = "The initial status of the evaluation")
        private String status;

        @Schema(title = "The display name of the evaluation")
        private String displayName;
    }
}
