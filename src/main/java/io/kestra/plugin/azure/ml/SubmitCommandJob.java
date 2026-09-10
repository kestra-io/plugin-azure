package io.kestra.plugin.azure.ml;

import java.net.URI;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;

import com.azure.core.management.exception.ManagementException;
import com.azure.resourcemanager.machinelearning.MachineLearningManager;
import com.azure.resourcemanager.machinelearning.models.CommandJob;
import com.azure.resourcemanager.machinelearning.models.JobBase;
import com.azure.resourcemanager.machinelearning.models.JobResourceConfiguration;
import com.azure.resourcemanager.machinelearning.models.UriFolderJobInput;
import com.azure.resourcemanager.machinelearning.models.UriFolderJobOutput;

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
                id: azure_ml_submit_command_job
                namespace: company.team

                tasks:
                  - id: train
                    type: io.kestra.plugin.azure.ml.SubmitCommandJob
                    tenantId: "{{ secret('AZURE_TENANT_ID') }}"
                    clientId: "{{ secret('AZURE_CLIENT_ID') }}"
                    clientSecret: "{{ secret('AZURE_CLIENT_SECRET') }}"
                    subscriptionId: "{{ secret('AZURE_SUBSCRIPTION_ID') }}"
                    resourceGroupName: ml-rg
                    workspaceName: ml-workspace
                    computeName: cpu-cluster
                    environmentId: "azureml:AzureML-sklearn-1.5:1"
                    command: "python train.py --epochs 10"

                  - id: check_accuracy
                    type: io.kestra.plugin.core.execution.Fail
                    condition: "{{ outputs.train.metrics['accuracy'] < 0.9 }}"
                """
        )
    }
)
@Schema(
    title = "Submit a command job to an Azure Machine Learning compute target",
    description = "Submits a single command job (e.g. a training script) to a compute cluster or instance and, by default, waits for it to reach a terminal state, exposing MLflow-backed metrics as task outputs. Killing the Kestra execution cancels the underlying Azure ML job. Defaults: wait=true, checkFrequency.interval=PT10S, checkFrequency.maxDuration=PT1H, cancelOnTimeout=true."
)
public class SubmitCommandJob extends AbstractMachineLearningTask implements RunnableTask<SubmitCommandJob.Output> {
    @Schema(title = "Job name", description = "Unique job name within the workspace; a random UUID is generated when not set")
    @PluginProperty(group = "main")
    private Property<String> name;

    @Schema(title = "Command", description = "Shell command executed on the compute target, e.g. `python train.py --epochs 10`")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> command;

    @Schema(title = "Compute target name", description = "Name of an existing Azure Machine Learning compute cluster or compute instance")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> computeName;

    @Schema(title = "Environment ID", description = "Registered environment reference, e.g. `azureml:AzureML-sklearn-1.5:1`; required by Azure Machine Learning to run the command")
    @PluginProperty(group = "main")
    private Property<String> environmentId;

    @Schema(title = "Instance count", description = "Number of compute nodes to use for this job; defaults to 1")
    @Builder.Default
    @PluginProperty(group = "main")
    private Property<Integer> instanceCount = Property.ofValue(1);

    @Schema(
        title = "Data inputs",
        description = "Named folder inputs available to the command as `${{inputs.<name>}}`; keys are input names, values are storage URIs such as `azureml://datastores/<store>/paths/<path>`."
    )
    @PluginProperty(group = "main")
    private Property<Map<String, String>> inputs;

    @Schema(
        title = "Data outputs",
        description = "Named folder outputs the command writes to via `${{outputs.<name>}}`; keys are output names, values are destination storage URIs."
    )
    @PluginProperty(group = "main")
    private Property<Map<String, String>> outputs;

    @Schema(title = "Experiment name", description = "Groups this run under an experiment in Azure ML Studio")
    @PluginProperty(group = "advanced")
    private Property<String> experimentName;

    @Schema(title = "Display name", description = "Human-readable run name shown in Azure ML Studio")
    @PluginProperty(group = "advanced")
    private Property<String> displayName;

    @Schema(title = "Wait for completion", description = "If true (default), poll the job status until it reaches a terminal state and collect metrics")
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<Boolean> wait = Property.ofValue(Boolean.TRUE);

    @Schema(
        title = "Cancel the job on timeout", description = "When `wait=true` and `checkFrequency.maxDuration` is exceeded, cancel the Azure ML job before failing the task; defaults to true"
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<Boolean> cancelOnTimeout = Property.ofValue(Boolean.TRUE);

    @Schema(title = "Polling frequency", description = "Interval and max duration used when `wait=true`")
    @Builder.Default
    @PluginProperty(group = "advanced")
    private CheckFrequency checkFrequency = CheckFrequency.builder().build();

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
        String rComputeName = runContext.render(this.computeName).as(String.class).orElseThrow();
        String rCommand = runContext.render(this.command).as(String.class).orElseThrow();
        String jobName = runContext.render(this.name).as(String.class).orElse(UUID.randomUUID().toString());

        MachineLearningManager manager = machineLearningManager(runContext);

        MachineLearningComputeService.ensureComputeUsable(manager, rResourceGroupName, rWorkspaceName, rComputeName);

        CommandJob commandJob = new CommandJob()
            .withCommand(rCommand)
            .withComputeId(rComputeName)
            .withResources(new JobResourceConfiguration().withInstanceCount(runContext.render(this.instanceCount).as(Integer.class).orElse(1)));

        String rEnvironmentId = runContext.render(this.environmentId).as(String.class).orElse(null);
        if (rEnvironmentId != null) {
            commandJob.withEnvironmentId(rEnvironmentId);
        }

        Map<String, String> rInputs = runContext.render(this.inputs).asMap(String.class, String.class);
        if (!rInputs.isEmpty()) {
            Map<String, com.azure.resourcemanager.machinelearning.models.JobInput> jobInputs = new HashMap<>();
            rInputs.forEach((inputName, inputUri) -> jobInputs.put(inputName, new UriFolderJobInput().withUri(inputUri)));
            commandJob.withInputs(jobInputs);
        }

        Map<String, String> rOutputs = runContext.render(this.outputs).asMap(String.class, String.class);
        if (!rOutputs.isEmpty()) {
            Map<String, com.azure.resourcemanager.machinelearning.models.JobOutput> jobOutputs = new HashMap<>();
            rOutputs.forEach((outputName, outputUri) -> jobOutputs.put(outputName, new UriFolderJobOutput().withUri(outputUri)));
            commandJob.withOutputs(jobOutputs);
        }

        runContext.render(this.experimentName).as(String.class).ifPresent(commandJob::withExperimentName);
        runContext.render(this.displayName).as(String.class).ifPresent(commandJob::withDisplayName);

        JobBase job;
        try {
            job = manager.jobs()
                .define(jobName)
                .withExistingWorkspace(rResourceGroupName, rWorkspaceName)
                .withProperties(commandJob)
                .create();
        } catch (ManagementException e) {
            throw translateSubmitError(e, jobName, rWorkspaceName, rComputeName);
        }

        logger.info("Submitted Azure Machine Learning command job '{}' on compute '{}'", jobName, rComputeName);

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
            throw new IllegalStateException("Job '%s' finished with status '%s' — check the run logs in Azure ML Studio: %s".formatted(jobName, state, studioUrl(runContext, jobName)));
        }
        logger.info("Job '{}' finished with status '{}'", jobName, state);

        Map<String, Double> metrics = MachineLearningService.mlflowMetrics(
            runContext,
            credentials(runContext),
            manager.workspaces().getByResourceGroup(rResourceGroupName, rWorkspaceName).mlFlowTrackingUri(),
            jobName
        );

        return Output.builder()
            .jobName(jobName)
            .status(state)
            .studioUrl(studioUrl(runContext, jobName))
            .metrics(metrics)
            .outputs(MachineLearningService.namedOutputs(finalJob.properties() instanceof CommandJob resolved ? resolved.outputs() : null))
            .build();
    }

    private static IllegalArgumentException translateSubmitError(ManagementException e, String jobName, String workspaceName, String computeName) {
        int statusCode = e.getResponse() != null ? e.getResponse().getStatusCode() : 0;
        if (statusCode == 409) {
            return new IllegalArgumentException(
                "Job '%s' already exists in workspace '%s' — Azure ML job names are unique per workspace; set a different `name` or leave it empty to auto-generate one"
                    .formatted(jobName, workspaceName),
                e
            );
        }
        if (statusCode == 404) {
            return new IllegalArgumentException(
                "Compute target '%s' or workspace '%s' was not found — check `computeName`, `resourceGroupName` and `workspaceName`".formatted(computeName, workspaceName), e
            );
        }
        return new IllegalArgumentException("Failed to submit job to compute '%s': %s".formatted(computeName, e.getValue() != null ? e.getValue().getMessage() : e.getMessage()), e);
    }

    @Override
    public void kill() {
        this.lifecycle.kill();
    }

    @Builder
    @Getter
    public static class CheckFrequency {
        @Schema(title = "Max wait duration", description = "Stop polling and fail after this duration; defaults to PT1H")
        @Builder.Default
        private Property<Duration> maxDuration = Property.ofValue(Duration.ofHours(1));

        @Schema(
            title = "Polling interval",
            description = "Delay between status checks; defaults to PT10S. Azure ML jobs run for minutes to hours, so a tighter interval only adds needless ARM API calls"
        )
        @Builder.Default
        private Property<Duration> interval = Property.ofValue(Duration.ofSeconds(10));
    }

    @SuperBuilder
    @Getter
    @NoArgsConstructor
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Job name", description = "Name of the submitted Azure Machine Learning job")
        private String jobName;

        @Schema(title = "Job status", description = "Terminal or last-observed job status")
        private JobState status;

        @Schema(title = "Studio URL", description = "Deep link to the run in Azure ML Studio")
        private String studioUrl;

        @Schema(
            title = "Metrics",
            description = """
                Metrics logged by the run, keyed by metric name. Always empty when `wait=false`, since the job has not \
                necessarily finished logging anything yet.

                Azure Machine Learning logs job metrics through MLflow, not through the ARM control-plane API used \
                for everything else in this task. This value is fetched best-effort by reading the workspace's MLflow \
                tracking URI and calling its REST API directly with the same Azure AD bearer token used to authenticate \
                this task. If that call fails (e.g. the service principal lacks the required scope, or the endpoint is \
                unreachable), a warning is logged and this field is an empty map — the task does not fail because of it.
                """
        )
        private Map<String, Double> metrics;

        @Schema(title = "Outputs", description = "Named job outputs, keyed by output name, pointing to their storage URI")
        private Map<String, URI> outputs;
    }
}
