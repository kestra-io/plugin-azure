package io.kestra.plugin.azure.logicapps;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import com.azure.core.credential.TokenCredential;
import com.azure.core.management.profile.AzureProfile;
import com.azure.core.util.Context;
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
class ListRunsTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldListRunsWithStatusFilter() throws Exception {
        WorkflowRuns runs = mock(WorkflowRuns.class);
        var workflowRun = LogicAppsTestHelper.run("id-1", "run-1", WorkflowStatus.FAILED);
        when(runs.list("rg", "workflow", 10, "status eq 'Failed'", Context.NONE))
            .thenReturn(LogicAppsTestHelper.paged(workflowRun));
        LogicManager manager = LogicAppsTestHelper.managerWithRuns(runs);

        ListRuns task = ListRuns.builder()
            .id("list-runs")
            .type(ListRuns.class.getName())
            .tenantId(Property.ofValue("tenant"))
            .clientId(Property.ofValue("client"))
            .clientSecret(Property.ofValue("secret"))
            .subscriptionId(Property.ofValue("subscription"))
            .resourceGroupName(Property.ofValue("rg"))
            .workflowName(Property.ofValue("workflow"))
            .statusFilter(Property.ofValue("Failed"))
            .maxRuns(Property.ofValue(10))
            .build();

        try (MockedStatic<LogicManager> mockedStatic = mockStatic(LogicManager.class)) {
            mockedStatic.when(() -> LogicManager.authenticate(any(TokenCredential.class), any(AzureProfile.class))).thenReturn(manager);

            ListRuns.Output output = task.run(runContextFactory.of());

            assertThat(output.getTotal(), is(1));
            assertThat(output.getRuns().getFirst().getStatus(), is("Failed"));
        }
    }

    @Test
    void shouldBuildODataStatusFilter() {
        assertThat(ListRuns.statusFilter("Succeeded"), is("status eq 'Succeeded'"));
    }
}
