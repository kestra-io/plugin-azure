package io.kestra.plugin.azure.batch.models;

import com.microsoft.azure.batch.protocol.models.JobAddParameter;
import com.microsoft.azure.batch.protocol.models.MetadataItem;
import com.microsoft.azure.batch.protocol.models.PoolInformation;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
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
        title = "A string that uniquely identifies the Job within the Account. ",
        description = "The ID can contain any combination of alphanumeric characters including hyphens and underscores, " +
            "and cannot contain more than 64 characters. The ID is case-preserving and case-insensitive " +
            "(that is, you may not have two IDs within an Account that differ only by case)."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    @Size(max=64)
    private String id;

    @Schema(
        title = "The display name for the Job.",
        description = "The display name need not be unique and can contain any Unicode characters up to a maximum length of 1024."
    )
    @PluginProperty(dynamic = true)
    @Size(max=1024)
    private String displayName;

    @Schema(
        title = "The priority of the Job.",
        description = "Priority values can range from -1000 to 1000, with -1000 being the lowest priority and 1000 being the highest priority. The default value is 0."
    )
    @PluginProperty(dynamic = false)
    private Integer priority;

    @Schema(
        title = "The maximum number of tasks that can be executed in parallel for the Job.",
        description = "The value of `maxParallelTasks` must be -1 or greater than 0, if specified. " +
            "If not specified, the default value is -1, which means there's no limit to the number of tasks that " +
            "can be run at once. You can update a job's `maxParallelTasks` after it has been created using the update job API."
    )
    @PluginProperty(dynamic = false)
    private Integer maxParallelTasks;

    @Schema(
        title = "Labels to attach to the created job."
    )
    @PluginProperty(dynamic = true)
    private Map<String, String> labels;


    public JobAddParameter to(RunContext runContext, PoolInformation poolInformation) throws IllegalVariableEvaluationException {
        return new JobAddParameter()
            .withId(runContext.render(this.id))
            .withDisplayName(runContext.render(this.displayName))
            .withPriority(this.priority)
            .withMaxParallelTasks(this.maxParallelTasks)
            .withPoolInfo(poolInformation)
            .withMetadata(
                Optional.ofNullable(labels)
                    .stream()
                    .map(throwFunction(runContext::renderMap))
                    .map(throwFunction(Map::entrySet))
                    .flatMap(Collection::stream)
                    .map(e -> new MetadataItem().withName(e.getKey()).withValue(e.getValue()))
                    .toList()
            );
    }
}
