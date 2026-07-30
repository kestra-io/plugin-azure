package io.kestra.plugin.azure.logicapps;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Optional;

import com.azure.resourcemanager.logic.models.Workflow;
import com.azure.resourcemanager.logic.models.WorkflowProvisioningState;
import com.azure.resourcemanager.logic.models.WorkflowState;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class WorkflowRecord {
    @Schema(title = "Workflow ID", description = "Azure resource ID of the Logic App workflow.")
    private final String id;

    @Schema(title = "Workflow name", description = "Name of the Logic App workflow.")
    private final String name;

    @Schema(title = "Location", description = "Azure region of the Logic App workflow.")
    private final String location;

    @Schema(title = "State", description = "Runtime state of the Logic App workflow.")
    private final String state;

    @Schema(title = "Provisioning state", description = "Provisioning state of the Logic App workflow.")
    private final String provisioningState;

    @Schema(title = "Version", description = "Workflow definition version.")
    private final String version;

    @Schema(title = "Access endpoint", description = "Azure Logic Apps access endpoint.")
    private final String accessEndpoint;

    @Schema(title = "Created time", description = "Time the workflow was created.")
    private final OffsetDateTime createdTime;

    @Schema(title = "Changed time", description = "Time the workflow was last changed.")
    private final OffsetDateTime changedTime;

    @Schema(title = "Tags", description = "Tags associated with the workflow.")
    private final Map<String, String> tags;

    static WorkflowRecord of(Workflow workflow) {
        return WorkflowRecord.builder()
            .id(workflow.id())
            .name(workflow.name())
            .location(Optional.ofNullable(workflow.regionName()).orElse(workflow.location()))
            .state(Optional.ofNullable(workflow.state()).map(WorkflowState::toString).orElse(null))
            .provisioningState(Optional.ofNullable(workflow.provisioningState()).map(WorkflowProvisioningState::toString).orElse(null))
            .version(workflow.version())
            .accessEndpoint(workflow.accessEndpoint())
            .createdTime(workflow.createdTime())
            .changedTime(workflow.changedTime())
            .tags(workflow.tags())
            .build();
    }
}
