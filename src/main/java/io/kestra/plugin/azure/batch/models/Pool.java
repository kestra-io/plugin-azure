package io.kestra.plugin.azure.batch.models;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Value;

import jakarta.validation.constraints.NotNull;

@Builder
@Value
public class Pool {
    @Schema(
        title = "Pool ID",
        description = "Unique within the account (<=64 chars, alphanumeric, hyphen, underscore); case-insensitive uniqueness"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    String id;

    @Schema(
        title = "Target dedicated nodes",
        description = "Set when autoscale is disabled; optional but at least one of dedicated or lowPriority should be provided"
    )
    @PluginProperty(dynamic = true)
    Integer targetDedicatedNodes;

    @Schema(
        title = "Target low-priority nodes",
        description = "Spot/low-priority node count when autoscale is disabled; optional counterpart to dedicated nodes"
    )
    @PluginProperty(dynamic = true)
    Integer targetLowPriorityNodes;
}
