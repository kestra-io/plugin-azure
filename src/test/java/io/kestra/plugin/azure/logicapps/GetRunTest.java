package io.kestra.plugin.azure.logicapps;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import com.azure.core.credential.TokenCredential;
import com.azure.core.management.profile.AzureProfile;
import com.azure.resourcemanager.logic.LogicManager;
import com.azure.resourcemanager.logic.models.WorkflowRuns;
import com.azure.resourcemanager.logic.models.WorkflowStatus;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@KestraTest
class GetRunTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldReturnRunDetails() throws Exception {
        WorkflowRuns runs = mock(WorkflowRuns.class);
        var workflowRun = LogicAppsTestHelper.run("id-1", "run-1", WorkflowStatus.SUCCEEDED);
        when(runs.get("rg", "workflow", "run-1")).thenReturn(workflowRun);
        LogicManager manager = LogicAppsTestHelper.managerWithRuns(runs);

        GetRun task = GetRun.builder()
            .id("get-run")
            .type(GetRun.class.getName())
            .tenantId(Property.ofValue("tenant"))
            .clientId(Property.ofValue("client"))
            .clientSecret(Property.ofValue("secret"))
            .subscriptionId(Property.ofValue("subscription"))
            .resourceGroupName(Property.ofValue("rg"))
            .workflowName(Property.ofValue("workflow"))
            .runId(Property.ofValue("run-1"))
            .build();

        try (MockedStatic<LogicManager> mockedStatic = mockStatic(LogicManager.class)) {
            mockedStatic.when(() -> LogicManager.authenticate(any(TokenCredential.class), any(AzureProfile.class))).thenReturn(manager);

            GetRun.Output output = task.run(runContextFactory.of());

            assertThat(output.getRun().getName(), is("run-1"));
            assertThat(output.getRun().getStatus(), is("Succeeded"));
            assertThat(output.getRun().getOutputs().get("result"), is("run-1-output"));
        }
    }
}
