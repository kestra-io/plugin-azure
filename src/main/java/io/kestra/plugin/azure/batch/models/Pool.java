package io.kestra.plugin.azure.batch.models;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Value;

import javax.validation.constraints.NotNull;

@Builder
@Value
public class Pool {
    @Schema(
        title = "A string that uniquely identifies the Pool within the Account.",
        description = "The ID can contain any combination of alphanumeric characters including hyphens and underscores, " +
            "and cannot contain more than 64 characters. The ID is case-preserving and case-insensitive " +
            "(that is, you may not have two Pool IDs within an Account that differ only by case)."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    String id;

    @Schema(
        title = "The desired number of dedicated Compute Nodes in the Pool.",
        description = "This property must not be specified if enableAutoScale is set to true. " +
            "If enableAutoScale is set to false, then you must set either targetDedicatedNodes, targetLowPriorityNodes, or both."
    )
    @PluginProperty(dynamic = true)
    Integer targetDedicatedNodes;

    @Schema(
        title = "The desired number of low-priority Compute Nodes in the Pool.",
        description = "This property must not be specified if enableAutoScale is set to true. " +
            "If enableAutoScale is set to false, then you must set either targetDedicatedNodes, targetLowPriorityNodes, " +
            "or both."
    )
    @PluginProperty(dynamic = true)
    Integer targetLowPriorityNodes;
}
