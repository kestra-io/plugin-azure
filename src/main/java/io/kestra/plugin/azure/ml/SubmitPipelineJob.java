package io.kestra.plugin.azure.ml;

import java.net.URI;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.slf4j.Logger;

import com.azure.core.management.exception.ManagementException;
import com.azure.resourcemanager.machinelearning.MachineLearningManager;
import com.azure.resourcemanager.machinelearning.models.ComponentContainerProperties;
import com.azure.resourcemanager.machinelearning.models.ComponentVersionProperties;
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
                        computeId: "/subscriptions/{{ secret('AZURE_SUBSCRIPTION_ID') }}/resourceGroups/ml-rg/providers/Microsoft.MachineLearningServices/workspaces/ml-workspace/computes/cpu-cluster"
                        command: "python prepare.py"
                        environmentId: "azureml:AzureML-sklearn-1.5:1"
                      train:
                        type: command
                        computeId: "/subscriptions/{{ secret('AZURE_SUBSCRIPTION_ID') }}/resourceGroups/ml-rg/providers/Microsoft.MachineLearningServices/workspaces/ml-workspace/computes/cpu-cluster"
                        command: "python train.py"
                        environmentId: "azureml:AzureML-sklearn-1.5:1"
                """
        )
    }
)
@Schema(
    title = "Submit a multi-step pipeline job to Azure Machine Learning",
    description = "Submits a pipeline job made of several child jobs (e.g. a data-preparation step followed by a training step) and, by default, waits for it to reach a terminal state. `jobs` is the raw pipeline job graph as accepted by the Azure Machine Learning REST API: a map of step name to step definition (`type`, `computeId`, `command`, `environmentId`, `inputs`, `outputs`, ...). Each step's `computeId` must be the compute's full ARM resource ID (`/subscriptions/.../resourceGroups/.../providers/Microsoft.MachineLearningServices/workspaces/.../computes/<name>`) — a bare compute name is rejected by the API. A `type: command` step that has no `componentId` gets a minimal Azure Machine Learning component automatically registered from its `command`/`environmentId` (named `<job name>-<step name>`, version `1`), since Azure's pipeline job API only accepts steps that reference a component, unlike a standalone command job — set `componentId` explicitly on a step to reference an existing component instead and skip auto-registration for it. Killing the Kestra execution cancels the whole pipeline job. Defaults: wait=true, checkFrequency.interval=PT10S, checkFrequency.maxDuration=PT1H, cancelOnTimeout=true."
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
        Optional<String> explicitName = runContext.render(this.name).as(String.class);
        String jobName = explicitName.orElseGet(() -> UUID.randomUUID().toString());

        MachineLearningManager manager = machineLearningManager(runContext);
        if (explicitName.isPresent()) {
            MachineLearningService.ensureJobNameAvailable(manager, rResourceGroupName, rWorkspaceName, jobName);
        }

        Map<String, Object> rJobs = registerMissingComponents(
            manager,
            rSubscriptionId(runContext),
            rResourceGroupName,
            rWorkspaceName,
            jobName,
            runContext.render(this.jobs).asMap(String.class, Object.class)
        );
        PipelineJob pipelineJob = new PipelineJob().withJobs(rJobs);

        runContext.render(this.experimentName).as(String.class).ifPresent(pipelineJob::withExperimentName);
        runContext.render(this.displayName).as(String.class).ifPresent(pipelineJob::withDisplayName);

        // The job name is known before submission, so arm the kill lifecycle now: a kill signal arriving while
        // create() is still in flight (including after Azure has already provisioned the job server-side but
        // before this call returns) is then captured instead of having nothing to act on.
        this.lifecycle.arm(() -> MachineLearningService.cancelQuietly(runContext, manager, rResourceGroupName, rWorkspaceName, jobName));

        JobBase job;
        boolean created = false;
        try {
            job = MachineLearningService.withTimeout(
                () -> manager.jobs()
                    .define(jobName)
                    .withExistingWorkspace(rResourceGroupName, rWorkspaceName)
                    .withProperties(pipelineJob)
                    .create(),
                Duration.ofMinutes(2),
                () -> "Submitting job '%s' did not complete within 2 minutes".formatted(jobName)
            );
            created = true;
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
        } finally {
            // A failure here does not prove the job was never created (create() is a long-running operation) —
            // confirm before disarming, so we never silently drop the only cancel path for a job that is, in
            // fact, running in Azure.
            if (!created) {
                MachineLearningService.disarmIfJobDoesNotExist(this.lifecycle, manager, rResourceGroupName, rWorkspaceName, jobName);
            }
        }

        logger.info("Submitted Azure Machine Learning pipeline job '{}'", jobName);

        if (!Boolean.TRUE.equals(runContext.render(this.wait).as(Boolean.class).orElseThrow())) {
            return Output.builder()
                .jobName(jobName)
                .status(MachineLearningService.toJobState(job))
                .studioUrl(studioUrl(runContext, jobName))
                .metrics(Map.of())
                .outputs(Map.of())
                .build();
        }

        Duration interval = runContext.render(this.checkFrequency.getInterval()).as(Duration.class).orElseThrow();
        Duration maxDuration = runContext.render(this.checkFrequency.getMaxDuration()).as(Duration.class).orElseThrow();
        boolean rCancelOnTimeout = runContext.render(this.cancelOnTimeout).as(Boolean.class).orElse(true);

        JobBase finalJob = MachineLearningService.awaitCompletion(runContext, manager, rResourceGroupName, rWorkspaceName, jobName, interval, maxDuration, rCancelOnTimeout);

        JobState state = MachineLearningService.toJobState(finalJob);
        if (state.isFailure()) {
            throw new IllegalStateException(
                "Pipeline job '%s' finished with status '%s' — check the run logs in Azure ML Studio: %s".formatted(jobName, state, studioUrl(runContext, jobName))
            );
        }
        logger.info("Pipeline job '{}' finished with status '{}'", jobName, state);

        Map<String, Double> metrics = MachineLearningService.mlflowMetrics(
            runContext,
            credentials(runContext),
            manager,
            rResourceGroupName,
            rWorkspaceName,
            jobName
        );

        return Output.builder()
            .jobName(jobName)
            .status(state)
            .studioUrl(studioUrl(runContext, jobName))
            .metrics(metrics)
            .outputs(MachineLearningService.namedOutputs(finalJob.properties() instanceof PipelineJob resolved ? resolved.outputs() : null))
            .build();
    }

    @Override
    public void kill() {
        this.lifecycle.kill();
    }

    /**
     * Azure's ARM pipeline job API rejects an inline {@code command}/{@code environmentId} step the way
     * {@link SubmitCommandJob} accepts one standalone — each {@code type: command} step must instead reference a
     * pre-registered {@code Component} ARM resource. The Python/CLI convenience layer auto-registers an anonymous
     * component behind the scenes for this exact case; this raw ARM SDK does not, so this does it here: for every
     * {@code command} step missing a {@code componentId} (a step that already sets one is left untouched), a
     * minimal component wrapping its {@code command}/{@code environmentId} is registered and swapped in as
     * {@code componentId}. Any other step {@code type} is passed through unchanged.
     * Package-private (not {@code private}) so it can be unit-tested directly against a mocked
     * {@link MachineLearningManager}, without needing live Azure credentials.
     */
    static Map<String, Object> registerMissingComponents(
        MachineLearningManager manager,
        String subscriptionId,
        String resourceGroupName,
        String workspaceName,
        String jobName,
        Map<String, Object> jobs) {
        Map<String, Object> result = new LinkedHashMap<>();
        jobs.forEach((stepKey, stepValue) ->
        {
            if (!(stepValue instanceof Map<?, ?> rawStep) || !"command".equals(rawStep.get("type")) || rawStep.containsKey("componentId")) {
                result.put(stepKey, stepValue);
                return;
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> step = new LinkedHashMap<>((Map<String, Object>) rawStep);
            Object command = step.remove("command");
            Object environmentId = step.remove("environmentId");
            if (command == null || environmentId == null) {
                throw new IllegalArgumentException(
                    "Pipeline step '%s' has `type: command` and no `componentId` — either set `componentId` to a pre-registered component, or provide both `command` and `environmentId` so one can be auto-registered"
                        .formatted(stepKey)
                );
            }

            String componentName = sanitizeComponentName(jobName + "-" + stepKey);
            String componentVersion = "1";

            getOrCreateComponentContainer(manager, resourceGroupName, workspaceName, componentName);
            getOrCreateComponentVersion(
                manager,
                resourceGroupName,
                workspaceName,
                componentName,
                componentVersion,
                Map.of(
                    "name", componentName,
                    "version", componentVersion,
                    "type", "command",
                    "command", command,
                    "environment", environmentId,
                    "inputs", Map.of(),
                    "outputs", Map.of()
                )
            );

            step.put(
                "componentId",
                "/subscriptions/%s/resourceGroups/%s/providers/Microsoft.MachineLearningServices/workspaces/%s/components/%s/versions/%s"
                    .formatted(subscriptionId, resourceGroupName, workspaceName, componentName, componentVersion)
            );
            result.put(stepKey, step);
        });
        return result;
    }

    /**
     * Azure Machine Learning asset names must start with a letter or digit and contain only letters, digits, `-`
     * or `_`, up to 255 characters — sanitized here instead of letting an arbitrary job/step name reach the ARM API
     * and fail with an opaque validation error.
     */
    static String sanitizeComponentName(String raw) {
        String sanitized = raw.replaceAll("[^a-zA-Z0-9_-]", "-");
        if (sanitized.isEmpty() || !Character.isLetterOrDigit(sanitized.charAt(0))) {
            sanitized = "c-" + sanitized;
        }
        return sanitized.length() > 255 ? sanitized.substring(0, 255) : sanitized;
    }

    private static void getOrCreateComponentContainer(MachineLearningManager manager, String resourceGroupName, String workspaceName, String componentName) {
        try {
            manager.componentContainers().get(resourceGroupName, workspaceName, componentName);
            return;
        } catch (ManagementException e) {
            if (e.getResponse() == null || e.getResponse().getStatusCode() != 404) {
                throw e;
            }
        }
        try {
            manager.componentContainers()
                .define(componentName)
                .withExistingWorkspace(resourceGroupName, workspaceName)
                .withProperties(new ComponentContainerProperties())
                .create();
        } catch (ManagementException e) {
            if (e.getResponse() == null || e.getResponse().getStatusCode() != 409) {
                throw e;
            }
            // A concurrent execution created the container between our get() and this create() — it exists now,
            // which is exactly what this method is asked to ensure.
        }
    }

    private static void getOrCreateComponentVersion(
        MachineLearningManager manager,
        String resourceGroupName,
        String workspaceName,
        String componentName,
        String componentVersion,
        Map<String, Object> componentSpec) {
        try {
            manager.componentVersions()
                .define(componentVersion)
                .withExistingComponent(resourceGroupName, workspaceName, componentName)
                .withProperties(new ComponentVersionProperties().withComponentSpec(componentSpec))
                .create();
        } catch (ManagementException e) {
            if (e.getResponse() == null || e.getResponse().getStatusCode() != 409) {
                throw e;
            }
            // A prior identical submission (e.g. a retried pipeline job under the same name) already registered
            // this exact component version — component reuse here has no lineage/immutability concerns the way
            // model/data versions do, so this is reused rather than treated as an error.
        }
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

        @Schema(
            title = "Metrics",
            description = """
                Metrics logged by the run, keyed by metric name. Always empty when `wait=false`, since the pipeline \
                has not necessarily finished logging anything yet.

                Azure Machine Learning logs job metrics through MLflow, not through the ARM control-plane API used \
                for everything else in this task. This value is fetched best-effort by reading the workspace's MLflow \
                tracking URI and calling its REST API directly with the same Azure AD bearer token used to authenticate \
                this task. If that call fails (e.g. the service principal lacks the required scope, or the endpoint is \
                unreachable), a warning is logged and this field is an empty map — the task does not fail because of it.
                """
        )
        private Map<String, Double> metrics;

        @Schema(title = "Outputs", description = "Named pipeline outputs, keyed by output name, pointing to their storage URI")
        private Map<String, URI> outputs;
    }
}
