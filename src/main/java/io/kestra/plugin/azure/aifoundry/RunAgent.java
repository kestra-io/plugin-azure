package io.kestra.plugin.azure.aifoundry;

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
    title = "Create and run an Azure AI Foundry agent, returning conversation result."
)
public class RunAgent extends AbstractAiFoundryTask implements RunnableTask<RunAgent.Output> {

    @Schema(title = "The agent ID to run.")
    @NotNull
    private Property<String> agentId;

    @Schema(title = "The prompt or message to send to the agent.")
    @NotNull
    private Property<String> prompt;

    @Override
    public Output run(RunContext runContext) throws Exception {
        runContext.logger().warn("Agent triggers are not fully supported in the current beta of Azure AI SDK for Java.");

        String agentIdRendered = runContext.render(this.agentId).as(String.class).orElseThrow();
        String promptRendered = runContext.render(this.prompt).as(String.class).orElseThrow();

        // Placeholder logic for execution
        String result = "Mocked execution for: " + promptRendered;

        return Output.builder()
            .result(result)
            .threadId("mock-thread-id")
            .runId("mock-run-id")
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The final result from the agent.")
        private String result;

        @Schema(title = "The thread ID.")
        private String threadId;

        @Schema(title = "The run ID.")
        private String runId;
    }
}
