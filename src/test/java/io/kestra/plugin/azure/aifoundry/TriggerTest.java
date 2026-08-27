package io.kestra.plugin.azure.aifoundry;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import com.azure.ai.projects.AIProjectClientBuilder;
import com.azure.ai.projects.EvaluationsClient;
import com.azure.ai.projects.models.Evaluation;
import com.azure.core.http.rest.PagedIterable;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@KestraTest
class TriggerTest {

    @Inject
    private RunContextFactory runContextFactory;

    @SuppressWarnings("unchecked")
    @Test
    void evaluate_terminalEvaluations_firesAndDeduplicates() throws Exception {
        Trigger trigger = Trigger.builder()
            .id("trigger-" + java.util.UUID.randomUUID())
            .type(Trigger.class.getName())
            .endpoint(Property.ofValue("https://test.api.azureml.ms/"))
            .stateKey(Property.ofValue("test-" + java.util.UUID.randomUUID()))
            .build();

        var ctx = TestsUtils.mockTrigger(runContextFactory, trigger);
        TriggerContext triggerContext = TriggerContext.builder()
            .namespace("company.team")
            .flowId("azure_ai_flow")
            .build();

        Evaluation eval1 = mock(Evaluation.class);
        when(eval1.getName()).thenReturn("eval-1");
        when(eval1.getStatus()).thenReturn("Completed");

        Evaluation eval2 = mock(Evaluation.class);
        when(eval2.getName()).thenReturn("eval-2");
        when(eval2.getStatus()).thenReturn("Running");

        PagedIterable<Evaluation> mockedIterable = mock(PagedIterable.class);
        when(mockedIterable.iterator())
            .thenAnswer(inv -> List.of(eval1, eval2).iterator());

        EvaluationsClient evalClient = mock(EvaluationsClient.class);
        when(evalClient.listEvaluations()).thenReturn(mockedIterable);

        try (MockedConstruction<AIProjectClientBuilder> ignored = Mockito.mockConstruction(AIProjectClientBuilder.class, (mock, context) ->
        {
            when(mock.endpoint(anyString())).thenReturn(mock);
            when(mock.credential(any())).thenReturn(mock);
            when(mock.buildEvaluationsClient()).thenReturn(evalClient);
        })) {
            // First poll: eval-1 is Completed (fires), eval-2 is Running (skipped)
            Optional<Execution> result1 = trigger.evaluate(ctx.getKey(), triggerContext);
            assertThat(result1.isPresent(), is(true));

            // Second poll: eval-1 deduplicated, eval-2 still Running -> no fire
            Optional<Execution> result2 = trigger.evaluate(ctx.getKey(), triggerContext);
            assertThat(result2.isEmpty(), is(true));

            // Third poll: eval-2 now Failed -> fires
            when(eval2.getStatus()).thenReturn("Failed");
            Optional<Execution> result3 = trigger.evaluate(ctx.getKey(), triggerContext);
            assertThat(result3.isPresent(), is(true));

            verify(evalClient, times(3)).listEvaluations();
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    void evaluate_nonTerminalOnly_doesNotFire() throws Exception {
        Trigger trigger = Trigger.builder()
            .id("trigger-" + java.util.UUID.randomUUID())
            .type(Trigger.class.getName())
            .endpoint(Property.ofValue("https://test.api.azureml.ms/"))
            .stateKey(Property.ofValue("test-" + java.util.UUID.randomUUID()))
            .build();

        var ctx = TestsUtils.mockTrigger(runContextFactory, trigger);
        TriggerContext triggerContext = TriggerContext.builder()
            .namespace("company.team")
            .flowId("azure_ai_flow")
            .build();

        Evaluation evalRunning = mock(Evaluation.class);
        when(evalRunning.getName()).thenReturn("eval-run");
        when(evalRunning.getStatus()).thenReturn("Running");

        PagedIterable<Evaluation> mockedIterable = mock(PagedIterable.class);
        when(mockedIterable.iterator())
            .thenAnswer(inv -> List.of(evalRunning).iterator());

        EvaluationsClient evalClient = mock(EvaluationsClient.class);
        when(evalClient.listEvaluations()).thenReturn(mockedIterable);

        try (MockedConstruction<AIProjectClientBuilder> ignored = Mockito.mockConstruction(AIProjectClientBuilder.class, (mock, context) ->
        {
            when(mock.endpoint(anyString())).thenReturn(mock);
            when(mock.credential(any())).thenReturn(mock);
            when(mock.buildEvaluationsClient()).thenReturn(evalClient);
        })) {
            Optional<Execution> result = trigger.evaluate(ctx.getKey(), triggerContext);
            assertThat(result.isEmpty(), is(true));
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    void evaluate_customStatuses_onlyMatchesConfigured() throws Exception {
        Trigger trigger = Trigger.builder()
            .id("trigger-" + java.util.UUID.randomUUID())
            .type(Trigger.class.getName())
            .endpoint(Property.ofValue("https://test.api.azureml.ms/"))
            .stateKey(Property.ofValue("test-" + java.util.UUID.randomUUID()))
            .statuses(Property.ofValue(List.of("Completed")))
            .build();

        var ctx = TestsUtils.mockTrigger(runContextFactory, trigger);
        TriggerContext triggerContext = TriggerContext.builder()
            .namespace("company.team")
            .flowId("azure_ai_flow")
            .build();

        Evaluation evalCompleted = mock(Evaluation.class);
        when(evalCompleted.getName()).thenReturn("eval-ok");
        when(evalCompleted.getStatus()).thenReturn("Completed");

        Evaluation evalFailed = mock(Evaluation.class);
        when(evalFailed.getName()).thenReturn("eval-bad");
        when(evalFailed.getStatus()).thenReturn("Failed");

        PagedIterable<Evaluation> mockedIterable = mock(PagedIterable.class);
        when(mockedIterable.iterator())
            .thenAnswer(inv -> List.of(evalCompleted, evalFailed).iterator());

        EvaluationsClient evalClient = mock(EvaluationsClient.class);
        when(evalClient.listEvaluations()).thenReturn(mockedIterable);

        try (MockedConstruction<AIProjectClientBuilder> ignored = Mockito.mockConstruction(AIProjectClientBuilder.class, (mock, context) ->
        {
            when(mock.endpoint(anyString())).thenReturn(mock);
            when(mock.credential(any())).thenReturn(mock);
            when(mock.buildEvaluationsClient()).thenReturn(evalClient);
        })) {
            // Only eval-ok (Completed) should fire; eval-bad (Failed) should be ignored
            Optional<Execution> result = trigger.evaluate(ctx.getKey(), triggerContext);
            assertThat(result.isPresent(), is(true));
        }
    }
}
