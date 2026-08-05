package io.kestra.plugin.azure.logicapps;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.azure.resourcemanager.logic.models.WorkflowOutputParameter;
import com.azure.resourcemanager.logic.models.WorkflowRun;
import com.azure.resourcemanager.logic.models.WorkflowStatus;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class RunRecord {
    @Schema(title = "Run ID", description = "Azure resource ID of the workflow run.")
    private final String id;

    @Schema(title = "Run name", description = "Name of the workflow run.")
    private final String name;

    @Schema(title = "Status", description = "Current status of the workflow run.")
    private final String status;

    @Schema(title = "Code", description = "Result code reported by Azure Logic Apps.")
    private final String code;

    @Schema(title = "Start time", description = "Workflow run start time.")
    private final OffsetDateTime startTime;

    @Schema(title = "End time", description = "Workflow run end time.")
    private final OffsetDateTime endTime;

    @Schema(title = "Correlation ID", description = "Correlation ID associated with the workflow run.")
    private final String correlationId;

    @Schema(title = "Error", description = "Error payload returned by Azure Logic Apps, when available.")
    private final Object error;

    @Schema(title = "Outputs", description = "Workflow run outputs keyed by output name.")
    private final Map<String, Object> outputs;

    public static RunRecord of(WorkflowRun run) {
        return RunRecord.builder()
            .id(run.id())
            .name(run.name())
            .status(Optional.ofNullable(run.status()).map(WorkflowStatus::toString).orElse(null))
            .code(run.code())
            .startTime(run.startTime())
            .endTime(run.endTime())
            .correlationId(run.correlationId())
            .error(run.error())
            .outputs(outputValues(run.outputs()))
            .build();
    }

    private static Map<String, Object> outputValues(Map<String, WorkflowOutputParameter> outputs) {
        if (outputs == null) {
            return Map.of();
        }

        return outputs.entrySet()
            .stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> entry.getValue() == null ? null : entry.getValue().value()
                )
            );
    }
}
