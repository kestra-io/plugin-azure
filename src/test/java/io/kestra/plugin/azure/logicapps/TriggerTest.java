package io.kestra.plugin.azure.logicapps;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import com.azure.core.credential.TokenCredential;
import com.azure.core.management.profile.AzureProfile;
import com.azure.resourcemanager.logic.LogicManager;
import com.azure.resourcemanager.logic.models.WorkflowRuns;
import com.azure.resourcemanager.logic.models.WorkflowStatus;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.StatefulTriggerInterface;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@KestraTest
class TriggerTest {
    @Inject
    private io.kestra.core.runners.RunContextFactory runContextFactory;

    @Test
    void shouldTriggerOnNewCompletedRunAndSkipDuplicates() throws Exception {
        WorkflowRuns runs = mock(WorkflowRuns.class);
        var workflowRun = LogicAppsTestHelper.run("id-1", "run-1", WorkflowStatus.SUCCEEDED);
        when(runs.list("rg", "workflow", 25, null, com.azure.core.util.Context.NONE))
            .thenReturn(LogicAppsTestHelper.paged(workflowRun));
        LogicManager manager = LogicAppsTestHelper.managerWithRuns(runs);

        io.kestra.plugin.azure.logicapps.Trigger trigger = io.kestra.plugin.azure.logicapps.Trigger.builder()
            .id("trigger")
            .type(io.kestra.plugin.azure.logicapps.Trigger.class.getName())
            .tenantId(Property.ofValue("tenant"))
            .clientId(Property.ofValue("client"))
            .clientSecret(Property.ofValue("secret"))
            .subscriptionId(Property.ofValue("subscription"))
            .resourceGroupName(Property.ofValue("rg"))
            .workflowName(Property.ofValue("workflow"))
            .stateKey(Property.ofValue("trigger-fire-once-" + io.kestra.core.utils.IdUtils.create()))
            .interval(Duration.ofSeconds(60))
            .build();

        Map.Entry<ConditionContext, io.kestra.core.scheduler.model.TriggerState> context = TestsUtils.mockTrigger(runContextFactory, trigger);

        try (MockedStatic<LogicManager> mockedStatic = mockStatic(LogicManager.class)) {
            mockedStatic.when(() -> LogicManager.authenticate(any(TokenCredential.class), any(AzureProfile.class))).thenReturn(manager);

            Optional<Execution> first = trigger.evaluate(context.getKey(), context.getValue().context());
            Optional<Execution> second = trigger.evaluate(context.getKey(), context.getValue().context());

            assertThat(first.isPresent(), is(true));
            assertThat(first.get().getTrigger().getVariables().get("total"), is(1));
            assertThat(((java.util.List<?>) first.get().getTrigger().getVariables().get("runs")).size(), is(1));
            assertThat(second.isEmpty(), is(true));
        }
    }

    @Test
    void shouldIgnoreRunsOutsideConfiguredStatuses() throws Exception {
        WorkflowRuns runs = mock(WorkflowRuns.class);
        var workflowRun = LogicAppsTestHelper.run("id-1", "run-1", WorkflowStatus.CANCELLED);
        when(runs.list("rg", "workflow", 25, null, com.azure.core.util.Context.NONE))
            .thenReturn(LogicAppsTestHelper.paged(workflowRun));
        LogicManager manager = LogicAppsTestHelper.managerWithRuns(runs);

        io.kestra.plugin.azure.logicapps.Trigger trigger = io.kestra.plugin.azure.logicapps.Trigger.builder()
            .id("trigger")
            .type(io.kestra.plugin.azure.logicapps.Trigger.class.getName())
            .tenantId(Property.ofValue("tenant"))
            .clientId(Property.ofValue("client"))
            .clientSecret(Property.ofValue("secret"))
            .subscriptionId(Property.ofValue("subscription"))
            .resourceGroupName(Property.ofValue("rg"))
            .workflowName(Property.ofValue("workflow"))
            .statuses(Property.ofValue(java.util.List.of("Succeeded")))
            .stateKey(Property.ofValue("trigger-status-filter-" + io.kestra.core.utils.IdUtils.create()))
            .on(Property.ofValue(StatefulTriggerInterface.On.CREATE))
            .interval(Duration.ofSeconds(60))
            .build();

        Map.Entry<ConditionContext, io.kestra.core.scheduler.model.TriggerState> context = TestsUtils.mockTrigger(runContextFactory, trigger);

        try (MockedStatic<LogicManager> mockedStatic = mockStatic(LogicManager.class)) {
            mockedStatic.when(() -> LogicManager.authenticate(any(TokenCredential.class), any(AzureProfile.class))).thenReturn(manager);

            Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue().context());

            assertThat(execution.isEmpty(), is(true));
        }
    }
}
