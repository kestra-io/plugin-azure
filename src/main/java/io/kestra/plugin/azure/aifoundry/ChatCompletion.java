package io.kestra.plugin.azure.aifoundry;

import java.util.ArrayList;
import java.util.List;

import com.azure.ai.inference.ChatCompletionsClient;
import com.azure.ai.inference.ChatCompletionsClientBuilder;
import com.azure.ai.inference.models.ChatCompletions;
import com.azure.ai.inference.models.ChatCompletionsOptions;
import com.azure.ai.inference.models.ChatRequestMessage;
import com.azure.ai.inference.models.ChatRequestSystemMessage;
import com.azure.ai.inference.models.ChatRequestUserMessage;
import com.azure.core.credential.KeyCredential;
import com.azure.core.credential.TokenCredential;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
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
            title = "Chat completion with a deployed model",
            code = """
                    id: azure_ai_chat_completion
                    namespace: company.team
                    inputs:
                      - id: prompt
                        type: STRING
                        defaults: "Summarize the quarterly report."
                    tasks:
                      - id: chat
                        type: io.kestra.plugin.azure.aifoundry.ChatCompletion
                        endpoint: "{{ secret('AZURE_AI_FOUNDRY_ENDPOINT') }}"
                        apiKey: "{{ secret('AZURE_AI_FOUNDRY_API_KEY') }}"
                        deploymentName: gpt-4o
                        messages:
                          - role: user
                            content: "{{ inputs.prompt }}"
                """
        )
    }
)
@Schema(
    title = "Call a deployed model for chat completions",
    description = "Use Azure AI Foundry to generate chat completions using a deployed model endpoint."
)
public class ChatCompletion extends AbstractAiFoundryTask implements RunnableTask<ChatCompletion.Output> {

    @Schema(title = "The name of the deployment to use")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> deploymentName;

    @Schema(title = "The messages to generate chat completions for")
    @NotNull
    @PluginProperty(group = "main")
    private Property<List<ChatMessage>> messages;

    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Builder
    public static class ChatMessage {
        @NotNull
        private Property<String> role;
        @NotNull
        private Property<String> content;
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        ChatCompletionsClientBuilder builder = new ChatCompletionsClientBuilder()
            .endpoint(this.getEndpoint(runContext));

        KeyCredential key = this.getKeyCredential(runContext);
        if (key != null) {
            builder.credential(key);
        } else {
            TokenCredential token = this.getTokenCredential(runContext);
            builder.credential(token);
        }

        ChatCompletionsClient client = builder.buildClient();

        String deployment = runContext.render(this.deploymentName).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("deploymentName is required"));
        List<ChatMessage> messageList = runContext.render(this.messages).asList(ChatMessage.class);

        List<ChatRequestMessage> requestMessages = new ArrayList<>();
        for (ChatMessage msg : messageList) {
            String role = runContext.render(msg.getRole()).as(String.class).orElseThrow();
            String content = runContext.render(msg.getContent()).as(String.class).orElseThrow();

            if ("system".equalsIgnoreCase(role)) {
                requestMessages.add(new ChatRequestSystemMessage(content));
            } else if ("user".equalsIgnoreCase(role)) {
                requestMessages.add(new ChatRequestUserMessage(content));
            } else {
                throw new IllegalArgumentException(
                    "Unsupported chat message role: '" + role + "'. Supported values: system, user."
                );
            }
        }

        ChatCompletionsOptions options = new ChatCompletionsOptions(requestMessages);
        options.setModel(deployment);

        ChatCompletions completions = client.complete(options);

        List<?> choices = completions.getChoices();
        if (choices == null || choices.isEmpty()) {
            throw new IllegalStateException(
                "Azure AI Foundry returned no choices for deployment '" + deployment +
                    "'. Verify the deployment is active and the prompt is not empty."
            );
        }
        String result = completions.getChoices().getFirst().getMessage().getContent();

        runContext.logger().info("Chat completion generated successfully via deployment {}.", deployment);

        return Output.builder()
            .content(result)
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The generated chat completion content")
        private String content;
    }
}
