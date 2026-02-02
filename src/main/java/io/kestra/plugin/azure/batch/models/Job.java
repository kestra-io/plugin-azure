package io.kestra.plugin.azure.batch.models;

import com.microsoft.azure.batch.protocol.models.JobAddParameter;
import com.microsoft.azure.batch.protocol.models.MetadataItem;
import com.microsoft.azure.batch.protocol.models.PoolInformation;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Value;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import static io.kestra.core.utils.Rethrow.throwFunction;

@Builder
@Value
public class Job {
    @Schema(
        title = "Job ID",
        description = "Unique within the Batch account (<=64 chars, alphanumeric, hyphen, underscore); case-insensitive uniqueness"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    @Size(max=64)
    private String id;

    @Schema(
        title = "Display name",
        description = "Optional friendly name (up to 1024 Unicode chars); not required to be unique"
    )
    @PluginProperty(dynamic = true)
    @Size(max=1024)
    private String displayName;

    @Schema(
        title = "Job priority",
        description = "Integer from -1000 to 1000; defaults to 0"
    )
    private Property<Integer> priority;

    @Schema(
        title = "Max parallel tasks",
        description = "Set to -1 for unlimited (default) or a positive integer; controls concurrent task scheduling"
    )
    private Property<Integer> maxParallelTasks;

    @Schema(
        title = "Job labels",
        description = "Key/value metadata applied to the job"
    )
    private Property<Map<String, String>> labels;


    public JobAddParameter to(RunContext runContext, PoolInformation poolInformation) throws IllegalVariableEvaluationException {
        return new JobAddParameter()
            .withId(runContext.render(this.id))
            .withDisplayName(runContext.render(this.displayName))
            .withPriority(runContext.render(this.priority).as(Integer.class).orElse(null))
            .withMaxParallelTasks(runContext.render(this.maxParallelTasks).as(Integer.class).orElse(null))
            .withPoolInfo(poolInformation)
            .withMetadata(
                runContext.render(this.labels).asMap(String.class, String.class)
                    .entrySet()
                    .stream()
                    .map(e -> new MetadataItem().withName(e.getKey()).withValue(e.getValue()))
                    .toList()
            );
    }
}
