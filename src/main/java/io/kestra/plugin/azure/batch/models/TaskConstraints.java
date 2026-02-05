package io.kestra.plugin.azure.batch.models;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Value;
import org.joda.time.Period;

import java.time.Duration;

@Builder
@Value
public class TaskConstraints {
    @Schema(
        title = "Max wall clock time",
        description = "Duration limit from task start; Batch terminates the task if it exceeds this. Null means no limit."
    )
    Property<Duration> maxWallClockTime;

    @Schema(
        title = "Retention time",
        description = "Minimum time to keep the task working directory after completion; default is 7 days"
    )
    Property<Duration> retentionTime;

    @Schema(
        title = "Max retry count",
        description = "Number of retries after non-zero exit: 0 disables retries; -1 retries indefinitely; default is Azure Batch default"
    )
    Property<Integer> maxTaskRetryCount;

    public com.microsoft.azure.batch.protocol.models.TaskConstraints to(RunContext runContext) throws IllegalVariableEvaluationException {
        return new com.microsoft.azure.batch.protocol.models.TaskConstraints()
            .withMaxWallClockTime(this.maxWallClockTime == null ? null : Period.parse(runContext.render(this.maxWallClockTime).as(Duration.class).orElseThrow().toString()))
            .withRetentionTime(this.retentionTime == null ? null : Period.parse(runContext.render(this.retentionTime).as(Duration.class).orElseThrow().toString()))
            .withMaxTaskRetryCount(runContext.render(this.maxTaskRetryCount).as(Integer.class).orElse(null));
    }
}
