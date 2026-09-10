package io.kestra.plugin.azure.ml;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;

import com.azure.core.management.exception.ManagementException;
import com.azure.resourcemanager.machinelearning.MachineLearningManager;
import com.azure.resourcemanager.machinelearning.models.JobBase;
import com.azure.resourcemanager.machinelearning.models.PipelineJob;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.AccessLevel;
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
                id: azure_ml_submit_pipeline_job
                namespace: company.team

                tasks:
                  - id: run_pipeline
                    type: io.kestra.plugin.azure.ml.SubmitPipelineJob
                    tenantId: "{{ secret('AZURE_TENANT_ID') }}"
                    clientId: "{{ secret('AZURE_CLIENT_ID') }}"
                    clientSecret: "{{ secret('AZURE_CLIENT_SECRET') }}"
                    subscriptionId: "{{ secret('AZURE_SUBSCRIPTION_ID') }}"
                    resourceGroupName: ml-rg
                    workspaceName: ml-workspace
                    jobs:
                      prepare:
                        type: command
                        computeId: cpu-cluster
                        command: "python prepare.py"
                        environmentId: "azureml:AzureML-sklearn-1.5:1"
                      train:
                        type: command
                        computeId: cpu-cluster
                        command: "python train.py"
                        environmentId: "azureml:AzureML-sklearn-1.5:1"
                """
        )
    }
)
@Schema(
    title = "Submit a multi-step pipeline job to Azure Machine Learning",
    description = "Submits a pipeline job made of several child jobs (e.g. a data-preparation step followed by a training step) and, by default, waits for it to reach a terminal state. `jobs` is the raw pipeline job graph as accepted by the Azure Machine Learning REST API: a map of step name to step definition (`type`, `computeId`, `command`, `environmentId`, `inputs`, `outputs`, ...). Killing the Kestra execution cancels the whole pipeline job. Defaults: wait=true, checkFrequency.interval=PT10S, checkFrequency.maxDuration=PT1H, cancelOnTimeout=true."
)
public class SubmitPipelineJob extends AbstractMachineLearningTask implements RunnableTask<SubmitPipelineJob.Output> {
    @Schema(title = "Job name", description = "Unique job name within the workspace; a random UUID is generated when not set")
    @PluginProperty(group = "main")
    private Property<String> name;

    @Schema(
        title = "Pipeline steps",
        description = "Raw pipeline job graph, as a map of step name to step definition, following the Azure Machine Learning REST API `jobs` schema for pipeline jobs."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<Map<String, Object>> jobs;

    @Schema(title = "Experiment name", description = "Groups this run under an experiment in Azure ML Studio")
    @PluginProperty(group = "advanced")
    private Property<String> experimentName;

    @Schema(title = "Display name", description = "Human-readable run name shown in Azure ML Studio")
    @PluginProperty(group = "advanced")
    private Property<String> displayName;

    @Schema(title = "Wait for completion", description = "If true (default), poll the pipeline status until it reaches a terminal state")
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<Boolean> wait = Property.ofValue(Boolean.TRUE);

    @Schema(
        title = "Cancel the pipeline on timeout",
        description = "When `wait=true` and `checkFrequency.maxDuration` is exceeded, cancel the Azure ML pipeline job before failing the task; defaults to true"
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<Boolean> cancelOnTimeout = Property.ofValue(Boolean.TRUE);

    @Schema(title = "Polling frequency", description = "Interval and max duration used when `wait=true`")
    @Builder.Default
    @PluginProperty(group = "advanced")
    private SubmitCommandJob.CheckFrequency checkFrequency = SubmitCommandJob.CheckFrequency.builder().build();

    @lombok.Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @Builder.Default
    private final CancellableJob lifecycle = new CancellableJob();

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        String rResourceGroupName = rResourceGroupName(runContext);
        String rWorkspaceName = rWorkspaceName(runContext);
        String jobName = runContext.render(this.name).as(String.class).orElse(UUID.randomUUID().toString());

        MachineLearningManager manager = machineLearningManager(runContext);

        PipelineJob pipelineJob = new PipelineJob()
            .withJobs(runContext.render(this.jobs).asMap(String.class, Object.class));

        runContext.render(this.experimentName).as(String.class).ifPresent(pipelineJob::withExperimentName);
        runContext.render(this.displayName).as(String.class).ifPresent(pipelineJob::withDisplayName);

        JobBase job;
        try {
            job = manager.jobs()
                .define(jobName)
                .withExistingWorkspace(rResourceGroupName, rWorkspaceName)
                .withProperties(pipelineJob)
                .create();
        } catch (ManagementException e) {
            int statusCode = e.getResponse() != null ? e.getResponse().getStatusCode() : 0;
            if (statusCode == 409) {
                throw new IllegalArgumentException(
                    "Job '%s' already exists in workspace '%s' — Azure ML job names are unique per workspace; set a different `name` or leave it empty to auto-generate one"
                        .formatted(jobName, rWorkspaceName),
                    e
                );
            }
            if (statusCode == 404) {
                throw new IllegalArgumentException(
                    "Workspace '%s' was not found, or one of the pipeline steps references a compute target or environment that doesn't exist — check `resourceGroupName`, `workspaceName` and each step's `computeId`/`environmentId`"
                        .formatted(rWorkspaceName),
                    e
                );
            }
            throw new IllegalArgumentException("Failed to submit pipeline job '%s': %s".formatted(jobName, e.getValue() != null ? e.getValue().getMessage() : e.getMessage()), e);
        }

        logger.info("Submitted Azure Machine Learning pipeline job '{}'", jobName);

        this.lifecycle.arm(() -> MachineLearningService.cancelQuietly(runContext, manager, rResourceGroupName, rWorkspaceName, jobName));

        if (!Boolean.TRUE.equals(runContext.render(this.wait).as(Boolean.class).orElseThrow())) {
            return Output.builder()
                .jobName(jobName)
                .status(MachineLearningService.toJobState(job.properties().status()))
                .studioUrl(studioUrl(runContext, jobName))
                .build();
        }

        Duration interval = runContext.render(this.checkFrequency.getInterval()).as(Duration.class).orElseThrow();
        Duration maxDuration = runContext.render(this.checkFrequency.getMaxDuration()).as(Duration.class).orElseThrow();
        boolean rCancelOnTimeout = runContext.render(this.cancelOnTimeout).as(Boolean.class).orElse(true);

        JobBase finalJob = MachineLearningService.awaitCompletion(runContext, manager, rResourceGroupName, rWorkspaceName, jobName, interval, maxDuration, rCancelOnTimeout);

        JobState state = MachineLearningService.toJobState(finalJob.properties().status());
        if (state.isFailure()) {
            throw new IllegalStateException(
                "Pipeline job '%s' finished with status '%s' — check the run logs in Azure ML Studio: %s".formatted(jobName, state, studioUrl(runContext, jobName))
            );
        }
        logger.info("Pipeline job '{}' finished with status '{}'", jobName, state);

        return Output.builder()
            .jobName(jobName)
            .status(state)
            .studioUrl(studioUrl(runContext, jobName))
            .outputs(MachineLearningService.namedOutputs(finalJob.properties() instanceof PipelineJob resolved ? resolved.outputs() : null))
            .build();
    }

    @Override
    public void kill() {
        this.lifecycle.kill();
    }

    @SuperBuilder
    @Getter
    @NoArgsConstructor
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Job name", description = "Name of the submitted Azure Machine Learning pipeline job")
        private String jobName;

        @Schema(title = "Job status", description = "Terminal or last-observed job status")
        private JobState status;

        @Schema(title = "Studio URL", description = "Deep link to the run in Azure ML Studio")
        private String studioUrl;

        @Schema(title = "Outputs", description = "Named pipeline outputs, keyed by output name, pointing to their storage URI")
        private Map<String, URI> outputs;
    }
}
