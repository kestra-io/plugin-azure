package io.kestra.plugin.azure.aifoundry;

import java.time.Duration;
import java.util.List;
import java.util.Set;

import com.azure.ai.agents.persistent.PersistentAgentsClient;
import com.azure.ai.agents.persistent.MessagesClient;
import com.azure.ai.agents.persistent.RunsClient;
import com.azure.ai.agents.persistent.ThreadsClient;
import com.azure.ai.agents.persistent.models.CreateRunOptions;
import com.azure.ai.agents.persistent.models.MessageContent;
import com.azure.ai.agents.persistent.models.MessageRole;
import com.azure.ai.agents.persistent.models.MessageTextContent;
import com.azure.ai.agents.persistent.models.PersistentAgentThread;
import com.azure.ai.agents.persistent.models.RunStatus;
import com.azure.ai.agents.persistent.models.ThreadMessage;
import com.azure.ai.agents.persistent.models.ThreadRun;
import com.azure.ai.projects.AIProjectClientBuilder;
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
            title = "Run an Azure AI Foundry agent and retrieve the reply",
            code = {
                "id: azure_ai_run_agent",
                "namespace: company.team",
                "tasks:",
                "  - id: run_agent",
                "    type: io.kestra.plugin.azure.aifoundry.RunAgent",
                "    endpoint: \"{{ secret('AZURE_AI_FOUNDRY_ENDPOINT') }}\"",
                "    agentId: asst_abc123",
                "    prompt: \"Summarize last week's sales data.\"",
                "    pollInterval: PT5S",
                "    timeout: PT5M"
            }
        )
    }
)
@Schema(
    title = "Create and run an Azure AI Foundry agent, returning the conversation result",
    description = "Creates a thread, posts the prompt as a user message, starts a run against " +
        "the specified agent, polls until the run reaches a terminal state, then returns the " +
        "last assistant message. Uses Entra ID authentication (DefaultAzureCredential)."
)
public class RunAgent extends AbstractAiFoundryTask implements RunnableTask<RunAgent.Output> {

    private static final Set<RunStatus> TERMINAL_STATUSES = Set.of(
        RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.CANCELLED, RunStatus.EXPIRED
    );
    private static final long DEFAULT_POLL_INTERVAL_MS = 5_000L;
    private static final long DEFAULT_TIMEOUT_MS = 300_000L; // 5 minutes

    @Schema(title = "The agent (assistant) ID to run")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> agentId;

    @Schema(title = "The prompt or message to send to the agent")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> prompt;

    @Schema(
        title = "How often to poll the run status",
        description = "ISO 8601 duration, e.g. PT5S. Defaults to 5 seconds."
    )
    @PluginProperty(group = "advanced")
    @Builder.Default
    private Property<Duration> pollInterval = Property.ofValue(Duration.ofSeconds(5));

    @Schema(
        title = "Maximum time to wait for the run to complete",
        description = "ISO 8601 duration, e.g. PT5M. Defaults to 5 minutes."
    )
    @PluginProperty(group = "advanced")
    @Builder.Default
    private Property<Duration> timeout = Property.ofValue(Duration.ofMinutes(5));

    @Override
    public Output run(RunContext runContext) throws Exception {
        String agentIdRendered = runContext.render(this.agentId)
            .as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("agentId is required"));
        String promptRendered = runContext.render(this.prompt)
            .as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("prompt is required"));
        long pollMs = runContext.render(this.pollInterval)
            .as(Duration.class)
            .orElse(Duration.ofMillis(DEFAULT_POLL_INTERVAL_MS))
            .toMillis();
        long timeoutMs = runContext.render(this.timeout)
            .as(Duration.class)
            .orElse(Duration.ofMillis(DEFAULT_TIMEOUT_MS))
            .toMillis();

        TokenCredential token = this.getTokenCredential(runContext);
        PersistentAgentsClient agentsClient = new AIProjectClientBuilder()
            .endpoint(this.getEndpoint(runContext))
            .credential(token)
            .buildPersistentAgentsClient();

        ThreadsClient threadsClient = agentsClient.getThreadsClient();
        MessagesClient messagesClient = agentsClient.getMessagesClient();
        RunsClient runsClient = agentsClient.getRunsClient();

        // 1. Create thread
        PersistentAgentThread thread = threadsClient.createThread();
        String threadId = thread.getId();
        runContext.logger().info("Created thread {}", threadId);

        // 2. Post user message
        messagesClient.createMessage(threadId, MessageRole.USER, promptRendered);
        runContext.logger().debug("Posted user message to thread {}", threadId);

        // 3. Create run
        ThreadRun threadRun = runsClient.createRun(new CreateRunOptions(threadId, agentIdRendered));
        String runId = threadRun.getId();
        runContext.logger().info("Created run {} on thread {}", runId, threadId);

        // 4. Poll until terminal
        long deadline = System.currentTimeMillis() + timeoutMs;
        RunStatus status = threadRun.getStatus();
        while (status == null || !TERMINAL_STATUSES.contains(status)) {
            if (System.currentTimeMillis() > deadline) {
                throw new IllegalStateException(
                    "Run " + runId + " did not complete within the configured timeout (" +
                    Duration.ofMillis(timeoutMs) + "). Last status: " + status + "."
                );
            }
            Thread.sleep(pollMs);
            threadRun = runsClient.getRun(threadId, runId);
            status = threadRun.getStatus();
            runContext.logger().debug("Run {} status: {}", runId, status);
        }

        if (!RunStatus.COMPLETED.equals(status)) {
            String errorMsg = threadRun.getLastError() != null
                ? threadRun.getLastError().getMessage()
                : "no error details available";
            throw new IllegalStateException(
                "Run " + runId + " finished with non-completed status " + status +
                ". Error: " + errorMsg
            );
        }

        // 5. Retrieve last assistant message
        List<ThreadMessage> messages = messagesClient.listMessages(threadId).stream().toList();
        String assistantReply = messages.stream()
            .filter(m -> MessageRole.AGENT.equals(m.getRole()))
            .findFirst()
            .map(m -> extractText(m.getContent()))
            .orElseThrow(() -> new IllegalStateException(
                "Run " + runId + " completed but no assistant message was found in thread " + threadId + "."
            ));

        runContext.logger().info("Run {} completed successfully.", runId);

        return Output.builder()
            .result(assistantReply)
            .threadId(threadId)
            .runId(runId)
            .build();
    }

    private String extractText(List<MessageContent> contents) {
        if (contents == null || contents.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (MessageContent content : contents) {
            if (content instanceof MessageTextContent textContent) {
                if (textContent.getText() != null) {
                    sb.append(textContent.getText().getValue());
                }
            }
        }
        return sb.toString();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The last assistant message from the agent")
        private String result;

        @Schema(title = "The thread ID created for this run")
        private String threadId;

        @Schema(title = "The run ID")
        private String runId;
    }
}
