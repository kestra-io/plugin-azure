package io.kestra.plugin.azure.storage.cosmosdb;

import com.azure.cosmos.models.PartitionKeyDefinitionVersion;
import com.azure.cosmos.models.PartitionKind;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;

import java.util.List;

public record PartitionKeyDefinition(
    @NotNull
    @Schema(
        title = "paths",
        description = "Sets the item property paths for the partition key."
    )
    List<String> paths,

    @NotNull
    @Schema(
        title = "kind",
        description = "Sets the partition algorithm used to calculate the partition id given a partition key"
    )
    PartitionKind kind,

    @NotNull
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