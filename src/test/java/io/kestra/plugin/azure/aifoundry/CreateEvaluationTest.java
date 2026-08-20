package io.kestra.plugin.azure.aifoundry;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import com.azure.ai.projects.AIProjectClientBuilder;
import com.azure.ai.projects.EvaluationsClient;
import com.azure.ai.projects.models.Evaluation;
import com.azure.ai.projects.models.InputDataset;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@KestraTest
class CreateEvaluationTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run_withMockedClient_returnsEvaluationDetailsAndVerifiesArgs() throws Exception {
        CreateEvaluation task = CreateEvaluation.builder()
            .id("create-evaluation")
            .type(CreateEvaluation.class.getName())
            .endpoint(Property.ofValue("https://test.api.azureml.ms/"))
            .datasetId(Property.ofValue("azureml:my-dataset:1"))
            .displayName(Property.ofValue("Test Eval"))
            .evaluators(Property.ofValue(Map.of("coherence", "azureml://some-evaluator")))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        Evaluation mockEvaluation = mock(Evaluation.class);
        when(mockEvaluation.getName()).thenReturn("eval-123");
        when(mockEvaluation.getStatus()).thenReturn("Running");
        when(mockEvaluation.getDisplayName()).thenReturn("Test Eval");

        EvaluationsClient mockClient = mock(EvaluationsClient.class);
        when(mockClient.createEvaluation(any(Evaluation.class))).thenReturn(mockEvaluation);

        try (MockedConstruction<AIProjectClientBuilder> ignored =
                 Mockito.mockConstruction(AIProjectClientBuilder.class, (mock, ctx) -> {
                     when(mock.endpoint(anyString())).thenReturn(mock);
                     when(mock.credential(any())).thenReturn(mock);
                     when(mock.buildEvaluationsClient()).thenReturn(mockClient);
                 })) {

            CreateEvaluation.Output output = task.run(runContext);

            assertThat(output, notNullValue());
            assertThat(output.getName(), is("eval-123"));
            assertThat(output.getStatus(), is("Running"));
            assertThat(output.getDisplayName(), is("Test Eval"));

            ArgumentCaptor<Evaluation> captor = ArgumentCaptor.forClass(Evaluation.class);
            verify(mockClient).createEvaluation(captor.capture());

            Evaluation sentEval = captor.getValue();
            assertThat(sentEval.getDisplayName(), is("Test Eval"));
            assertThat(((InputDataset) sentEval.getData()).getId(), is("azureml:my-dataset:1"));
            assertThat(sentEval.getEvaluators().containsKey("coherence"), is(true));
            assertThat(sentEval.getEvaluators().get("coherence").getId(), is("azureml://some-evaluator"));
        }
    }
}
