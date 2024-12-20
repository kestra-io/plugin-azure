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
        title = "The maximum elapsed time that the Task may run, measured from the time the Task starts.",
        description = "If the Task does not complete within the time limit, the Batch service terminates it. " +
            "If this is not specified, there is no time limit on how long the Task may run."
    )
    Property<Duration> maxWallClockTime;

    @Schema(
        title = "The minimum time to retain the Task directory on the Compute Node where it ran, from the time it completes execution.",
        description = "After this time, the Batch service may delete the Task directory and all its contents. " +
            "The default is 7 days, i.e. the Task directory will be retained for 7 days unless " +
            "the Compute Node is removed or the Job is deleted."
    )
    Property<Duration> retentionTime;

    @Schema(
        title = "The maximum number of times the Task may be retried.",
        description = "The Batch service retries a Task if its exit code is nonzero. Note that this value specifically " +
            "controls the number of retries for the Task executable due to a nonzero exit code. The Batch service will " +
            "try the Task once, and may then retry up to this limit. For example, if the maximum retry count is 3, " +
            "Batch tries the Task up to 4 times (one initial try and 3 retries). If the maximum retry count is 0, the" +
            " Batch service does not retry the Task after the first attempt. If the maximum retry count is -1, " +
            "the Batch service retries the Task without limit."
    )
    Property<Integer> maxTaskRetryCount;

    public com.microsoft.azure.batch.protocol.models.TaskConstraints to(RunContext runContext) throws IllegalVariableEvaluationException {
        return new com.microsoft.azure.batch.protocol.models.TaskConstraints()
            .withMaxWallClockTime(this.maxWallClockTime == null ? null : Period.parse(runContext.render(this.maxWallClockTime).as(Duration.class).orElseThrow().toString()))
            .withRetentionTime(this.retentionTime == null ? null : Period.parse(runContext.render(this.retentionTime).as(Duration.class).orElseThrow().toString()))
            .withMaxTaskRetryCount(runContext.render(this.maxTaskRetryCount).as(Integer.class).orElse(null));
    }
}
