package io.kestra.plugin.azure.aifoundry;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Optional;

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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;

@KestraTest
class TriggerTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void evaluate_completedRun_firesExecutionAndDeduplicatesAndVerifiesArgs() throws Exception {
        Trigger trigger = Trigger.builder()
            .id("trigger")
            .type(Trigger.class.getName())
            .endpoint(Property.ofValue("https://test.api.azureml.ms/"))
            .threadId(Property.ofValue("thread-123"))
            .runId(Property.ofValue("run-456"))
            .stateKey(Property.ofValue("trigger-completed-test"))
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
            System.out.println("RES1: " + result1); assertThat(result1.isPresent(), is(true));

            // Second poll: Same run and status, should be deduplicated (return Optional.empty)
            Optional<Execution> result2 = trigger.evaluate(ctx.getKey(), ctx.getValue());
            assertThat(result2.isEmpty(), is(true));

            // Verify specific thread and run IDs were checked
            verify(runsClient, times(2)).getRun("thread-123", "run-456");
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
            .stateKey(Property.ofValue("trigger-in-progress-test"))
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
    void evaluate_failedRun_firesExecution() throws Exception {
        Trigger trigger = Trigger.builder()
            .id("trigger")
            .type(Trigger.class.getName())
            .endpoint(Property.ofValue("https://test.api.azureml.ms/"))
            .threadId(Property.ofValue("thread-123"))
            .runId(Property.ofValue("run-456"))
            .stateKey(Property.ofValue("trigger-failed-test"))
            .build();

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> ctx =
            TestsUtils.mockTrigger(runContextFactory, trigger);

        ThreadRun mockRun = mock(ThreadRun.class);
        when(mockRun.getStatus()).thenReturn(RunStatus.FAILED);
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

            Optional<Execution> result = trigger.evaluate(ctx.getKey(), ctx.getValue());
            assertThat(result.isPresent(), is(true));
        }
    }
}
