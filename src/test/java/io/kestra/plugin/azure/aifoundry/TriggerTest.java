package io.kestra.plugin.azure.aifoundry;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import com.azure.ai.projects.AIProjectClientBuilder;
import com.azure.ai.projects.EvaluationsClient;
import com.azure.ai.projects.models.Evaluation;
import com.azure.core.http.rest.PagedIterable;
import com.azure.core.http.rest.RequestOptions;
import com.azure.core.util.BinaryData;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;
import jakarta.validation.ConstraintViolationException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

        PagedIterable<BinaryData> mockedIterable = mock(PagedIterable.class);
        when(mockedIterable.iterator())
            .thenAnswer(inv -> List.of(binaryDataFor(eval1), binaryDataFor(eval2)).iterator());

        EvaluationsClient evalClient = mock(EvaluationsClient.class);
        when(evalClient.listEvaluations(any(RequestOptions.class))).thenReturn(mockedIterable);

        try (MockedConstruction<AIProjectClientBuilder> ignored = Mockito.mockConstruction(AIProjectClientBuilder.class, (mock, context) ->
        {
            when(mock.endpoint(anyString())).thenReturn(mock);
            when(mock.credential(any())).thenReturn(mock);
            when(mock.buildEvaluationsClient()).thenReturn(evalClient);
        })) {
            // First poll: eval-1 is Completed (fires), eval-2 is Running (skipped)
            Optional<Execution> result1 = trigger.evaluate(ctx.getKey(), triggerContext);
            assertThat(result1.isPresent(), is(true));
            assertThat(result1.get().getTrigger().getVariables().get("total"), is(1));
            assertEvaluation(result1.get(), "eval-1", "Completed");

            // Second poll: eval-1 deduplicated, eval-2 still Running -> no fire
            Optional<Execution> result2 = trigger.evaluate(ctx.getKey(), triggerContext);
            assertThat(result2.isEmpty(), is(true));

            // Third poll: eval-2 now Failed -> fires
            when(eval2.getStatus()).thenReturn("Failed");
            Optional<Execution> result3 = trigger.evaluate(ctx.getKey(), triggerContext);
            assertThat(result3.isPresent(), is(true));
            assertThat(result3.get().getTrigger().getVariables().get("total"), is(1));
            assertEvaluation(result3.get(), "eval-2", "Failed");

            verify(evalClient, times(3)).listEvaluations(any(RequestOptions.class));
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

        PagedIterable<BinaryData> mockedIterable = mock(PagedIterable.class);
        when(mockedIterable.iterator())
            .thenAnswer(inv -> List.of(binaryDataFor(evalRunning)).iterator());

        EvaluationsClient evalClient = mock(EvaluationsClient.class);
        when(evalClient.listEvaluations(any(RequestOptions.class))).thenReturn(mockedIterable);

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

        PagedIterable<BinaryData> mockedIterable = mock(PagedIterable.class);
        when(mockedIterable.iterator())
            .thenAnswer(inv -> List.of(binaryDataFor(evalCompleted), binaryDataFor(evalFailed)).iterator());

        EvaluationsClient evalClient = mock(EvaluationsClient.class);
        when(evalClient.listEvaluations(any(RequestOptions.class))).thenReturn(mockedIterable);

        try (MockedConstruction<AIProjectClientBuilder> ignored = Mockito.mockConstruction(AIProjectClientBuilder.class, (mock, context) ->
        {
            when(mock.endpoint(anyString())).thenReturn(mock);
            when(mock.credential(any())).thenReturn(mock);
            when(mock.buildEvaluationsClient()).thenReturn(evalClient);
        })) {
            // Only eval-ok (Completed) should fire; eval-bad (Failed) should be ignored
            Optional<Execution> result = trigger.evaluate(ctx.getKey(), triggerContext);
            assertThat(result.isPresent(), is(true));
            assertThat(result.get().getTrigger().getVariables().get("total"), is(1));
            assertEvaluation(result.get(), "eval-ok", "Completed");
            assertThat(((List<?>) result.get().getTrigger().getVariables().get("evaluations")).size(), is(1));
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    void evaluate_maxEvaluations_onlyInspectsConfiguredLimit() throws Exception {
        Trigger trigger = Trigger.builder()
            .id("trigger-" + java.util.UUID.randomUUID())
            .type(Trigger.class.getName())
            .endpoint(Property.ofValue("https://test.api.azureml.ms/"))
            .stateKey(Property.ofValue("test-" + java.util.UUID.randomUUID()))
            .maxEvaluations(Property.ofValue(1))
            .build();

        var ctx = TestsUtils.mockTrigger(runContextFactory, trigger);
        TriggerContext triggerContext = TriggerContext.builder()
            .namespace("company.team")
            .flowId("azure_ai_flow")
            .build();

        Evaluation eval1 = mock(Evaluation.class);
        when(eval1.getName()).thenReturn("eval-first");
        when(eval1.getStatus()).thenReturn("Completed");

        // The Azure API applies the $top query param server-side, so a maxEvaluations of 1
        // means the (mocked) service itself only ever returns a single evaluation.
        PagedIterable<BinaryData> mockedIterable = mock(PagedIterable.class);
        when(mockedIterable.iterator())
            .thenAnswer(inv -> List.of(binaryDataFor(eval1)).iterator());

        EvaluationsClient evalClient = mock(EvaluationsClient.class);
        when(evalClient.listEvaluations(any(RequestOptions.class))).thenReturn(mockedIterable);

        try (MockedConstruction<AIProjectClientBuilder> ignored = Mockito.mockConstruction(AIProjectClientBuilder.class, (mock, context) ->
        {
            when(mock.endpoint(anyString())).thenReturn(mock);
            when(mock.credential(any())).thenReturn(mock);
            when(mock.buildEvaluationsClient()).thenReturn(evalClient);
        })) {
            Optional<Execution> result = trigger.evaluate(ctx.getKey(), triggerContext);
            assertThat(result.isPresent(), is(true));
            assertThat(result.get().getTrigger().getVariables().get("total"), is(1));
            assertEvaluation(result.get(), "eval-first", "Completed");
        }
    }

    @Test
    void evaluate_emptyStatuses_throws() {
        Trigger trigger = Trigger.builder()
            .id("trigger-" + java.util.UUID.randomUUID())
            .type(Trigger.class.getName())
            .endpoint(Property.ofValue("https://test.api.azureml.ms/"))
            .stateKey(Property.ofValue("test-" + java.util.UUID.randomUUID()))
            .statuses(Property.ofValue(List.of()))
            .build();

        var ctx = TestsUtils.mockTrigger(runContextFactory, trigger);
        TriggerContext triggerContext = TriggerContext.builder()
            .namespace("company.team")
            .flowId("azure_ai_flow")
            .build();

        ConstraintViolationException ex = assertThrows(
            ConstraintViolationException.class,
            () -> trigger.evaluate(ctx.getKey(), triggerContext)
        );
        assertThat(ex.getMessage(), containsString("statuses: must not be empty"));
    }

    @Test
    void evaluate_zeroMaxEvaluations_throws() {
        Trigger trigger = Trigger.builder()
            .id("trigger-" + java.util.UUID.randomUUID())
            .type(Trigger.class.getName())
            .endpoint(Property.ofValue("https://test.api.azureml.ms/"))
            .stateKey(Property.ofValue("test-" + java.util.UUID.randomUUID()))
            .maxEvaluations(Property.ofValue(0))
            .build();

        var ctx = TestsUtils.mockTrigger(runContextFactory, trigger);
        TriggerContext triggerContext = TriggerContext.builder()
            .namespace("company.team")
            .flowId("azure_ai_flow")
            .build();

        ConstraintViolationException ex = assertThrows(
            ConstraintViolationException.class,
            () -> trigger.evaluate(ctx.getKey(), triggerContext)
        );
        assertThat(ex.getMessage(), containsString("maxEvaluations: must be greater than or equal to 1"));
    }

    private BinaryData binaryDataFor(Evaluation evaluation) {
        BinaryData binaryData = mock(BinaryData.class);
        when(binaryData.toObject(Evaluation.class)).thenReturn(evaluation);
        return binaryData;
    }

    @SuppressWarnings("unchecked")
    private void assertEvaluation(Execution execution, String expectedName, String expectedStatus) {
        Object evaluation = execution.getTrigger().getVariables().get("evaluation");
        if (evaluation instanceof Trigger.EvaluationRecord record) {
            assertThat(record.getName(), is(expectedName));
            assertThat(record.getStatus(), is(expectedStatus));
            return;
        }

        Map<String, Object> record = (Map<String, Object>) evaluation;
        assertThat(record.get("name"), is(expectedName));
        assertThat(record.get("status"), is(expectedStatus));
    }
}
