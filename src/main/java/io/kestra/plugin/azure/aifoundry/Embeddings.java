package io.kestra.plugin.azure.aifoundry;

import java.util.List;

import com.azure.ai.inference.EmbeddingsClient;
import com.azure.ai.inference.EmbeddingsClientBuilder;
import com.azure.ai.inference.models.EmbeddingItem;
import com.azure.ai.inference.models.EmbeddingsResult;
import com.azure.core.credential.KeyCredential;

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
            title = "Generate embeddings and log the result",
            code = """
                    id: azure_ai_embeddings
                    namespace: company.team
                    tasks:
                      - id: embed
                        type: io.kestra.plugin.azure.aifoundry.Embeddings
                        endpoint: "{{ secret('AZURE_AI_FOUNDRY_ENDPOINT') }}"
                        apiKey: "{{ secret('AZURE_AI_FOUNDRY_API_KEY') }}"
                        deploymentName: text-embedding-3-small
                        inputs:
                          - "The quick brown fox jumps over the lazy dog."
                      - id: log_result
                        type: io.kestra.plugin.core.log.Log
                        message: "Embedding vector length: {{ outputs.embed.embeddings[0] | length }}"
                """
        )
    }
)
@Schema(
    title = "Generate vector embeddings from text input",
    description = "Use Azure AI Foundry to generate embeddings using a deployed model endpoint."
)
public class Embeddings extends AbstractAiFoundryTask implements RunnableTask<Embeddings.Output> {

    @Schema(title = "The name of the deployment to use")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> deploymentName;

    @Schema(title = "The text inputs to generate embeddings for")
    @NotNull
    @PluginProperty(group = "main")
    private Property<List<String>> inputs;

    @Override
    public Output run(RunContext runContext) throws Exception {
        EmbeddingsClientBuilder builder = new EmbeddingsClientBuilder()
            .endpoint(getEndpoint(runContext));

        KeyCredential key = getKeyCredential(runContext);
        if (key != null) {
            builder.credential(key);
        } else {
            builder.credential(getTokenCredential(runContext));
        }

        EmbeddingsClient client = builder.buildClient();

        String deployment = runContext.render(this.deploymentName)
            .as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("deploymentName is required"));

        List<String> inputTexts = runContext.render(this.inputs).asList(String.class);

        EmbeddingsResult result = client.embed(
            inputTexts, // inputs
            null, // dimensions
            null, // user
            null, // model
            null, // extraParameters
            null // requestOptions
        );

        List<List<Float>> embeddings = result.getData()
            .stream()
            .map(EmbeddingItem::getEmbeddingList)
            .toList();

        runContext.logger().info(
            "Generated {} embeddings using deployment {}.",
            embeddings.size(),
            deployment
        );

        return Output.builder()
            .embeddings(embeddings)
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The generated embeddings, one list of floats per input text")
        private List<List<Float>> embeddings;
    }
}
