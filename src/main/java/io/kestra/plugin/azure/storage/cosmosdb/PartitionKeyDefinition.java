package io.kestra.plugin.azure.storage.cosmosdb;

import com.azure.cosmos.models.PartitionKeyDefinitionVersion;
import com.azure.cosmos.models.PartitionKind;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;

import java.util.List;

public record PartitionKeyDefinition(
    @NotNull
    @Schema(
        title = "Partition key paths",
        description = "Ordered item property paths (e.g. [/pk]); each must start with '/'."
    )
    List<String> paths,

    @NotNull
    @Schema(
        title = "Partitioning algorithm",
        description = "Partition strategy, typically HASH, matching the container configuration."
    )
    PartitionKind kind,

    @NotNull
    @Schema(
        title = "Partition key definition version",
        description = "Version used when the container was created; choose the same value."
    )
    PartitionKeyDefinitionVersion version
) {
    com.azure.cosmos.models.PartitionKeyDefinition toAzurePartitionKeyDefinition() {
        var partitionKeyDefinition = new com.azure.cosmos.models.PartitionKeyDefinition();
        partitionKeyDefinition.setPaths(paths);
        partitionKeyDefinition.setKind(kind);
        partitionKeyDefinition.setVersion(version);
        return partitionKeyDefinition;
    }
}
