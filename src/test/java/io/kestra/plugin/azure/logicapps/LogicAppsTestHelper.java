package io.kestra.plugin.azure.logicapps;

import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Map;

import com.azure.core.http.HttpHeaders;
import com.azure.core.http.rest.PagedIterable;
import com.azure.core.http.rest.PagedResponseBase;
import com.azure.resourcemanager.logic.LogicManager;
import com.azure.resourcemanager.logic.models.Workflow;
import com.azure.resourcemanager.logic.models.WorkflowOutputParameter;
import com.azure.resourcemanager.logic.models.WorkflowProvisioningState;
import com.azure.resourcemanager.logic.models.WorkflowRun;
import com.azure.resourcemanager.logic.models.WorkflowRuns;
import com.azure.resourcemanager.logic.models.WorkflowState;
import com.azure.resourcemanager.logic.models.WorkflowStatus;
import com.azure.resourcemanager.logic.models.WorkflowTriggers;
import com.azure.resourcemanager.logic.models.Workflows;

import static org.mockito.Mockito.*;

final class LogicAppsTestHelper {
    private LogicAppsTestHelper() {
    }

    static LogicManager managerWithRuns(WorkflowRuns runs) {
        LogicManager manager = mock(LogicManager.class);
        when(manager.workflowRuns()).thenReturn(runs);
        return manager;
    }

    static LogicManager managerWithWorkflows(Workflows workflows) {
        LogicManager manager = mock(LogicManager.class);
        when(manager.workflows()).thenReturn(workflows);
        return manager;
    }

    static LogicManager managerWithTriggers(WorkflowTriggers triggers) {
        LogicManager manager = mock(LogicManager.class);
        when(manager.workflowTriggers()).thenReturn(triggers);
        return manager;
    }

    @SuppressWarnings("unchecked")
    static <T> PagedIterable<T> paged(T... items) {
        return new PagedIterable<>(
            () -> new PagedResponseBase<>(
                null,
                200,
                new HttpHeaders(),
                Arrays.asList(items),
                null,
                null
            )
        );
    }

    static WorkflowRun run(String id, String name, WorkflowStatus status) {
        WorkflowRun run = mock(WorkflowRun.class);
        doReturn(id).when(run).id();
        doReturn(name).when(run).name();
        doReturn(status).when(run).status();
        doReturn(status == WorkflowStatus.FAILED ? "BadGateway" : "OK").when(run).code();
        doReturn(OffsetDateTime.parse("2026-07-29T10:00:00Z")).when(run).startTime();
        doReturn(OffsetDateTime.parse("2026-07-29T10:01:00Z")).when(run).endTime();
        doReturn("corr-" + name).when(run).correlationId();
        doReturn(Map.of("result", new WorkflowOutputParameter().withValue(name + "-output"))).when(run).outputs();
        return run;
    }

    static Workflow workflow(String id, String name) {
        Workflow workflow = mock(Workflow.class);
        doReturn(id).when(workflow).id();
        doReturn(name).when(workflow).name();
        doReturn("eastus").when(workflow).regionName();
        doReturn(WorkflowState.ENABLED).when(workflow).state();
        doReturn(WorkflowProvisioningState.SUCCEEDED).when(workflow).provisioningState();
        doReturn("1").when(workflow).version();
        doReturn("https://management.azure.com/" + id).when(workflow).accessEndpoint();
        doReturn(OffsetDateTime.parse("2026-07-29T09:00:00Z")).when(workflow).createdTime();
        doReturn(OffsetDateTime.parse("2026-07-29T09:30:00Z")).when(workflow).changedTime();
        doReturn(Map.of("env", "test")).when(workflow).tags();
        return workflow;
    }
}
