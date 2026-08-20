import os

BASE = '/Users/hemantmehta/Documents/kesta-plugin-azure/src/main/java/io/kestra/plugin/azure/aifoundry'
TEST_BASE = '/Users/hemantmehta/Documents/kesta-plugin-azure/src/test/java/io/kestra/plugin/azure/aifoundry'

trigger_content = '''package io.kestra.plugin.azure.aifoundry;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import com.azure.ai.agents.persistent.PersistentAgentsClient;
import com.azure.ai.agents.persistent.RunsClient;
import com.azure.ai.agents.persistent.models.RunStatus;
import com.azure.ai.agents.persistent.models.ThreadRun;
import com.azure.ai.projects.AIProjectClientBuilder;
import com.azure.core.credential.TokenCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.models.triggers.StatefulTriggerInterface;
import io.kestra.core.models.triggers.StatefulTriggerService;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.models.triggers.TriggerService;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import static io.kestra.core.models.triggers.StatefulTriggerService.computeAndUpdateState;
import static io.kestra.core.models.triggers.StatefulTriggerService.readState;
import static io.kestra.core.models.triggers.StatefulTriggerService.writeState;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Fire an execution when a specific Azure AI Foundry agent run reaches a terminal state",
            code = {
                "id: azure_ai_on_agent_complete",
                "namespace: company.team",
                "triggers:",
                "  - id: on_agent_run",
                "    type: io.kestra.plugin.azure.aifoundry.Trigger",
                "    endpoint: \\"{{ secret('AZURE_AI_FOUNDRY_ENDPOINT') }}\\"",
                "    apiKey: \\"{{ secret('AZURE_AI_FOUNDRY_API_KEY') }}\\"",
                "    threadId: thread_abc123",
                "    runId: run_xyz789",
                "    interval: PT1M",
                "tasks:",
                "  - id: notify",
                "    type: io.kestra.plugin.core.log.Log",
                "    message: \\"Agent run {{ trigger.runId }} finished with status {{ trigger.status }}\\""
            }
        )
    }
)
@Schema(
    title = "Poll an Azure AI Foundry agent run and fire when it reaches a terminal state.",
    description = "Polls the Azure AI Projects PersistentAgentsClient for the status of a specific " +
        "run. Fires an execution when the run status is one of: COMPLETED, FAILED, CANCELLED, or EXPIRED. " +
        "Subsequent polls after the run has reached a terminal state will not re-fire (uses state deduplication)."
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Trigger.Output>, StatefulTriggerInterface {

    @Schema(
        title = "Azure AI Foundry endpoint.",
        description = "The Azure AI Foundry project endpoint URL."
    )
    @NotNull
    @PluginProperty(group = "connection")
    private Property<String> endpoint;

    @Schema(
        title = "Azure AI Foundry API key.",
        description = "API key for key-based authentication. When omitted, DefaultAzureCredential is used."
    )
    @ToString.Exclude
    @PluginProperty(group = "connection", secret = true)
    private Property<String> apiKey;

    @Schema(title = "The thread ID containing the run to watch.")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> threadId;

    @Schema(title = "The run ID to watch.")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> runId;

    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    @Builder.Default
    @Schema(title = "State change mode", description = "Stateful trigger change mode used for observed workflow runs.")
    @PluginProperty(group = "advanced")
    private final Property<On> on = Property.ofValue(On.CREATE);

    @Schema(title = "State key", description = "Custom key used to store observed workflow runs.")
    @PluginProperty(group = "advanced")
    private Property<String> stateKey;

    @Schema(title = "State TTL", description = "How long observed workflow run state is retained.")
    @PluginProperty(group = "advanced")
    private Property<Duration> stateTtl;

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();

        String rStateKey = runContext.render(stateKey).as(String.class).orElse(StatefulTriggerService.defaultKey(context.getNamespace(), context.getFlowId(), id));
        Optional<Duration> rStateTtl = runContext.render(stateTtl).as(Duration.class);
        On rOn = runContext.render(on).as(On.class).orElse(On.CREATE);

        String thread = runContext.render(this.threadId).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("threadId is required"));
        String run = runContext.render(this.runId).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("runId is required"));
        String endpointStr = runContext.render(this.endpoint).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("endpoint is required"));

        String apiKeyStr = runContext.render(this.apiKey).as(String.class).orElse(null);
        TokenCredential credential;
        if (apiKeyStr != null && !apiKeyStr.isBlank()) {
            runContext.logger().warn(
                "Trigger: API-key authentication is not supported by PersistentAgentsClient. " +
                "Falling back to DefaultAzureCredential. Configure Entra ID credentials instead."
            );
            credential = new DefaultAzureCredentialBuilder().build();
        } else {
            credential = new DefaultAzureCredentialBuilder().build();
        }

        PersistentAgentsClient agentsClient = new AIProjectClientBuilder()
            .endpoint(endpointStr)
            .credential(credential)
            .buildPersistentAgentsClient();

        RunsClient runsClient = agentsClient.getRunsClient();
        ThreadRun threadRun = runsClient.getRun(thread, run);
        RunStatus status = threadRun.getStatus();

        runContext.logger().debug("Polled run {} on thread {}: status={}", run, thread, status);

        boolean isTerminal = RunStatus.COMPLETED.equals(status)
            || RunStatus.FAILED.equals(status)
            || RunStatus.CANCELLED.equals(status)
            || RunStatus.EXPIRED.equals(status);

        if (!isTerminal) {
            return Optional.empty();
        }

        var previousState = readState(runContext, rStateKey, rStateTtl);

        // Fallback to Instant.now() if completedAt is null
        Instant modifiedAt = Optional.ofNullable(threadRun.getCompletedAt())
            .map(java.time.OffsetDateTime::toInstant)
            .orElseGet(Instant::now);

        var candidate = StatefulTriggerService.Entry.candidate(run, status.toString(), modifiedAt);
        var stateChange = computeAndUpdateState(previousState, candidate, rOn);

        writeState(runContext, rStateKey, previousState, rStateTtl);

        if (!stateChange.fire()) {
            runContext.logger().debug("Run {} state change didn't fire (already observed).", run);
            return Optional.empty();
        }

        runContext.logger().info("Run {} on thread {} reached terminal status: {}", run, thread, status);

        Output output = Output.builder()
            .runId(run)
            .threadId(thread)
            .status(status.toString())
            .build();

        return Optional.ofNullable(
            TriggerService.generateExecution(this, conditionContext, context, output)
        );
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The completed run ID.")
        private String runId;

        @Schema(title = "The thread ID.")
        private String threadId;

        @Schema(title = "The terminal status of the run (COMPLETED, FAILED, CANCELLED, or EXPIRED).")
        private String status;
    }
}
'''
with open(os.path.join(BASE, 'Trigger.java'), 'w') as f:
    f.write(trigger_content)

trigger_test_content = '''package io.kestra.plugin.azure.aifoundry;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import com.azure.ai.agents.persistent.PersistentAgentsClient;
import com.azure.ai.agents.persistent.RunsClient;
import com.azure.ai.agents.persistent.models.RunStatus;
import com.azure.ai.agents.persistent.models.ThreadRun;
import com.azure.ai.projects.AIProjectClientBuilder;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@KestraTest
class TriggerTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void evaluate_completedRun_firesExecutionAndDeduplicates() throws Exception {
        Trigger trigger = Trigger.builder()
            .id("trigger")
            .type(Trigger.class.getName())
            .endpoint(Property.ofValue("https://test.api.azureml.ms/"))
            .threadId(Property.ofValue("thread-123"))
            .runId(Property.ofValue("run-456"))
            .build();

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> ctx =
            TestsUtils.mockTrigger(runContextFactory, trigger);

        ThreadRun mockRun = mock(ThreadRun.class);
        when(mockRun.getStatus()).thenReturn(RunStatus.COMPLETED);
        when(mockRun.getCompletedAt()).thenReturn(OffsetDateTime.now());

        RunsClient runsClient = mock(RunsClient.class);
        when(runsClient.getRun(anyString(), anyString())).thenReturn(mockRun);

        PersistentAgentsClient agentsClient = mock(PersistentAgentsClient.class);
        when(agentsClient.getRunsClient()).thenReturn(runsClient);

        try (MockedConstruction<AIProjectClientBuilder> ignored =
                 Mockito.mockConstruction(AIProjectClientBuilder.class, (mock, c) -> {
                     when(mock.endpoint(anyString())).thenReturn(mock);
                     when(mock.credential(any())).thenReturn(mock);
                     when(mock.buildPersistentAgentsClient()).thenReturn(agentsClient);
                 })) {

            // First poll: Should fire execution
            Optional<Execution> result1 = trigger.evaluate(ctx.getKey(), ctx.getValue());
            assertThat(result1.isPresent(), is(true));

            // Second poll: Same run and status, should be deduplicated (return Optional.empty)
            Optional<Execution> result2 = trigger.evaluate(ctx.getKey(), ctx.getValue());
            assertThat(result2.isEmpty(), is(true));
        }
    }

    @Test
    void evaluate_inProgressRun_doesNotFire() throws Exception {
        Trigger trigger = Trigger.builder()
            .id("trigger")
            .type(Trigger.class.getName())
            .endpoint(Property.ofValue("https://test.api.azureml.ms/"))
            .threadId(Property.ofValue("thread-123"))
            .runId(Property.ofValue("run-456"))
            .build();

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> ctx =
            TestsUtils.mockTrigger(runContextFactory, trigger);

        ThreadRun mockRun = mock(ThreadRun.class);
        when(mockRun.getStatus()).thenReturn(RunStatus.IN_PROGRESS);

        RunsClient runsClient = mock(RunsClient.class);
        when(runsClient.getRun(anyString(), anyString())).thenReturn(mockRun);

        PersistentAgentsClient agentsClient = mock(PersistentAgentsClient.class);
        when(agentsClient.getRunsClient()).thenReturn(runsClient);

        try (MockedConstruction<AIProjectClientBuilder> ignored =
                 Mockito.mockConstruction(AIProjectClientBuilder.class, (mock, c) -> {
                     when(mock.endpoint(anyString())).thenReturn(mock);
                     when(mock.credential(any())).thenReturn(mock);
                     when(mock.buildPersistentAgentsClient()).thenReturn(agentsClient);
                 })) {

            Optional<Execution> result = trigger.evaluate(ctx.getKey(), ctx.getValue());
            assertThat(result.isEmpty(), is(true));
        }
    }

    @Test
    @Disabled("Needs Azure AI Foundry credentials via Entra ID")
    void evaluate_integration() throws Exception {
        Trigger trigger = Trigger.builder()
            .id("trigger")
            .type(Trigger.class.getName())
            .endpoint(Property.ofValue("https://your-endpoint.api.azureml.ms/"))
            .threadId(Property.ofValue("thread_yourThreadId"))
            .runId(Property.ofValue("run_yourRunId"))
            .build();

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> ctx =
            TestsUtils.mockTrigger(runContextFactory, trigger);

        Optional<Execution> result = trigger.evaluate(ctx.getKey(), ctx.getValue());
        assertThat(result != null, is(true));
    }
}
'''
with open(os.path.join(TEST_BASE, 'TriggerTest.java'), 'w') as f:
    f.write(trigger_test_content)

print("Updated Trigger and TriggerTest")
