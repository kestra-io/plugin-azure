package io.kestra.plugin.azure.ml;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

import com.azure.core.management.exception.ManagementException;
import com.azure.resourcemanager.machinelearning.MachineLearningManager;
import com.azure.resourcemanager.machinelearning.models.JobBase;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: azure_ml_cancel_job
                namespace: company.team

                inputs:
                  - id: job_name
                    type: STRING

                tasks:
                  - id: cancel_job
                    type: io.kestra.plugin.azure.ml.CancelJob
                    tenantId: "{{ secret('AZURE_TENANT_ID') }}"
                    clientId: "{{ secret('AZURE_CLIENT_ID') }}"
                    clientSecret: "{{ secret('AZURE_CLIENT_SECRET') }}"
                    subscriptionId: "{{ secret('AZURE_SUBSCRIPTION_ID') }}"
                    resourceGroupName: ml-rg
                    workspaceName: ml-workspace
                    jobName: "{{ inputs.job_name }}"
                """
        )
    }
)
@Schema(
    title = "Cancel an Azure Machine Learning job",
    description = "Requests cancellation of a running job. Cancellation is asynchronous in Azure ML: the job can sit in `CANCEL_REQUESTED` for minutes, so by default this task polls until the job actually reaches a terminal state before returning. Cancelling an already-terminal job is treated as a no-op, not a failure."
)
public class CancelJob extends AbstractMachineLearningTask implements RunnableTask<CancelJob.Output> {
    @Schema(title = "Job name", description = "Name of the Azure Machine Learning job to cancel")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> jobName;

    @Schema(
        title = "Wait for the job to reach a terminal state",
        description = "If true (default), polls until the job is actually cancelled instead of returning as soon as cancellation is requested"
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<Boolean> wait = Property.ofValue(Boolean.TRUE);

    @Schema(title = "Polling frequency", description = "Interval and max duration used when `wait=true`")
    @Builder.Default
    @PluginProperty(group = "advanced")
    private SubmitCommandJob.CheckFrequency checkFrequency = SubmitCommandJob.CheckFrequency.builder().build();

    @Override
    public Output run(RunContext runContext) throws Exception {
        var logger = runContext.logger();
        String rResourceGroupName = rResourceGroupName(runContext);
        String rWorkspaceName = rWorkspaceName(runContext);
        String rJobName = runContext.render(this.jobName).as(String.class).orElseThrow();

        MachineLearningManager manager = machineLearningManager(runContext);

        try {
            manager.jobs().cancel(rResourceGroupName, rWorkspaceName, rJobName);
            logger.info("Cancellation requested for Azure Machine Learning job '{}'", rJobName);
        } catch (ManagementException e) {
            int statusCode = e.getResponse() != null ? e.getResponse().getStatusCode() : 0;
            if (statusCode == 404) {
                throw new IllegalArgumentException(
                    "Job '%s' was not found in workspace '%s' — check `jobName`, `resourceGroupName` and `workspaceName`".formatted(rJobName, rWorkspaceName), e
                );
            }
            if (statusCode == 400 || statusCode == 409) {
                logger.warn("Job '{}' could not be cancelled, it is likely already in a terminal state: {}", rJobName, e.getMessage());
                JobBase current = manager.jobs().get(rResourceGroupName, rWorkspaceName, rJobName);
                return Output.builder()
                    .jobName(rJobName)
                    .status(MachineLearningService.toJobState(current.properties().status()))
                    .build();
            }
            throw e;
        }

        if (!Boolean.TRUE.equals(runContext.render(this.wait).as(Boolean.class).orElseThrow())) {
            return Output.builder()
                .jobName(rJobName)
                .status(JobState.CANCEL_REQUESTED)
                .build();
        }

        Duration interval = runContext.render(this.checkFrequency.getInterval()).as(Duration.class).orElseThrow();
        Duration maxDuration = runContext.render(this.checkFrequency.getMaxDuration()).as(Duration.class).orElseThrow();

        JobBase finalJob;
        try {
            finalJob = MachineLearningService.awaitTerminalState(
                () -> manager.jobs().get(rResourceGroupName, rWorkspaceName, rJobName),
                interval,
                maxDuration
            );
        } catch (TimeoutException e) {
            throw new IllegalStateException("Job '%s' did not reach a terminal state within %s after cancellation was requested".formatted(rJobName, maxDuration));
        }

        JobState state = MachineLearningService.toJobState(finalJob.properties().status());
        logger.info("Job '{}' is now in terminal state '{}'", rJobName, state);

        return Output.builder()
            .jobName(rJobName)
            .status(state)
            .build();
    }

    @SuperBuilder
    @Getter
    @NoArgsConstructor
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Job name")
        private String jobName;

        @Schema(title = "Job status", description = "Job status after the cancellation request")
        private JobState status;
    }
}
