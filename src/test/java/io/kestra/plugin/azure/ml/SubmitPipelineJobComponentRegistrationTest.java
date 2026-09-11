package io.kestra.plugin.azure.ml;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.azure.core.http.HttpResponse;
import com.azure.core.management.exception.ManagementException;
import com.azure.resourcemanager.machinelearning.MachineLearningManager;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Reproduces a live QA finding: submitting {@link SubmitPipelineJob}'s own documented example against a real Azure
 * ML workspace fails with "job prepare with type command should reference a component" — Azure's pipeline job API
 * (unlike {@link SubmitCommandJob}'s standalone job API) rejects an inline `command`/`environmentId` step and
 * requires each step to reference a pre-registered `Component` ARM resource instead. These are pure unit tests
 * against a mocked {@link MachineLearningManager}, so they run unconditionally in CI without live Azure credentials.
 */
class SubmitPipelineJobComponentRegistrationTest {
    @Test
    void registerMissingComponentsReplacesCommandAndEnvironmentIdWithComponentId() {
        MachineLearningManager manager = mock(MachineLearningManager.class, RETURNS_DEEP_STUBS);
        ManagementException containerNotFound = notFound();
        ManagementException versionNotFound = notFound();
        when(manager.componentContainers().get(anyString(), anyString(), anyString())).thenThrow(containerNotFound);
        when(manager.componentVersions().get(anyString(), anyString(), anyString(), anyString())).thenThrow(versionNotFound);

        Map<String, Object> jobs = Map.of(
            "prepare", Map.of(
                "type", "command",
                "computeId", "/subscriptions/sub-id/resourceGroups/ml-rg/providers/Microsoft.MachineLearningServices/workspaces/ml-workspace/computes/cpu-cluster",
                "command", "python prepare.py",
                "environmentId", "azureml:AzureML-sklearn-1.5:1"
            )
        );

        Map<String, Object> transformed = SubmitPipelineJob.registerMissingComponents(manager, "sub-id", "ml-rg", "ml-workspace", "my-job", jobs);

        @SuppressWarnings("unchecked")
        Map<String, Object> step = (Map<String, Object>) transformed.get("prepare");
        assertThat(step.containsKey("command"), is(false));
        assertThat(step.containsKey("environmentId"), is(false));
        assertThat(step.get("type"), is("command"));
        assertThat(step.get("computeId"), is("/subscriptions/sub-id/resourceGroups/ml-rg/providers/Microsoft.MachineLearningServices/workspaces/ml-workspace/computes/cpu-cluster"));
        assertThat(
            step.get("componentId"),
            is("/subscriptions/sub-id/resourceGroups/ml-rg/providers/Microsoft.MachineLearningServices/workspaces/ml-workspace/components/my-job-prepare/versions/1")
        );
    }

    @Test
    void registerMissingComponentsReusesAnExistingComponentVersionInsteadOfFailingOn409() {
        MachineLearningManager manager = mock(MachineLearningManager.class, RETURNS_DEEP_STUBS);
        ManagementException containerNotFound = notFound();
        ManagementException containerConflict = conflict();
        ManagementException versionConflict = conflict();
        when(manager.componentContainers().get(anyString(), anyString(), anyString())).thenThrow(containerNotFound);
        when(manager.componentContainers().define(anyString()).withExistingWorkspace(anyString(), anyString()).withProperties(any()).create())
            .thenThrow(containerConflict);
        when(
            manager.componentVersions()
                .define(anyString())
                .withExistingComponent(anyString(), anyString(), anyString())
                .withProperties(any())
                .create()
        ).thenThrow(versionConflict);

        Map<String, Object> jobs = Map.of(
            "train", Map.of("type", "command", "computeId", "cpu-cluster", "command", "python train.py", "environmentId", "azureml:AzureML-sklearn-1.5:1")
        );

        Map<String, Object> transformed = SubmitPipelineJob.registerMissingComponents(manager, "sub-id", "ml-rg", "ml-workspace", "my-job", jobs);

        @SuppressWarnings("unchecked")
        Map<String, Object> step = (Map<String, Object>) transformed.get("train");
        assertThat(step.get("componentId"), is(notNullValue()));
    }

    @Test
    void registerMissingComponentsLeavesAStepWithAnExplicitComponentIdUntouched() {
        MachineLearningManager manager = mock(MachineLearningManager.class);
        Map<String, Object> step = Map.of("type", "command", "componentId", "already-registered-component", "computeId", "cpu-cluster");
        Map<String, Object> jobs = Map.of("prepare", step);

        Map<String, Object> transformed = SubmitPipelineJob.registerMissingComponents(manager, "sub-id", "ml-rg", "ml-workspace", "my-job", jobs);

        assertThat(transformed.get("prepare"), is(sameInstance(step)));
        verifyNoInteractions(manager);
    }

    @Test
    void registerMissingComponentsLeavesNonCommandStepsUntouched() {
        MachineLearningManager manager = mock(MachineLearningManager.class);
        Map<String, Object> step = Map.of("type", "sweep");
        Map<String, Object> jobs = Map.of("tune", step);

        Map<String, Object> transformed = SubmitPipelineJob.registerMissingComponents(manager, "sub-id", "ml-rg", "ml-workspace", "my-job", jobs);

        assertThat(transformed.get("tune"), is(sameInstance(step)));
        verifyNoInteractions(manager);
    }

    @Test
    void registerMissingComponentsFailsWithAnActionableMessageWhenCommandOrEnvironmentIdAreMissing() {
        MachineLearningManager manager = mock(MachineLearningManager.class);
        Map<String, Object> step = new HashMap<>();
        step.put("type", "command");
        step.put("computeId", "cpu-cluster");
        Map<String, Object> jobs = Map.of("prepare", step);

        var exception = assertThrows(
            IllegalArgumentException.class,
            () -> SubmitPipelineJob.registerMissingComponents(manager, "sub-id", "ml-rg", "ml-workspace", "my-job", jobs)
        );

        assertThat(exception.getMessage(), containsString("prepare"));
        assertThat(exception.getMessage(), containsString("componentId"));
    }

    @Test
    void sanitizeComponentNameReplacesInvalidCharacters() {
        assertThat(SubmitPipelineJob.sanitizeComponentName("my-job_prepare.step"), is("my-job_prepare-step"));
    }

    @Test
    void sanitizeComponentNamePrefixesANonAlphanumericLeadingCharacter() {
        assertThat(SubmitPipelineJob.sanitizeComponentName("_leading-underscore"), is("c-_leading-underscore"));
    }

    @Test
    void sanitizeComponentNameTruncatesToMaxLength() {
        String tooLong = "a".repeat(300);

        assertThat(SubmitPipelineJob.sanitizeComponentName(tooLong).length(), is(255));
    }

    private static ManagementException notFound() {
        HttpResponse response = mock(HttpResponse.class);
        when(response.getStatusCode()).thenReturn(404);
        return new ManagementException("not found", response);
    }

    private static ManagementException conflict() {
        HttpResponse response = mock(HttpResponse.class);
        when(response.getStatusCode()).thenReturn(409);
        return new ManagementException("conflict", response);
    }
}
