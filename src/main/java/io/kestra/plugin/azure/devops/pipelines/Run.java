package io.kestra.plugin.azure.devops.pipelines;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import io.kestra.core.http.HttpResponse;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.CancellableOperation;

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
            title = "Run an Azure DevOps pipeline and wait for completion",
            full = true,
            code = """
                id: azure_devops_run_pipeline
                namespace: company.team

                tasks:
                  - id: run_pipeline
                    type: io.kestra.plugin.azure.devops.pipelines.Run
                    organization: my-org
                    project: my-project
                    pipelineId: 42
                    personalAccessToken: "{{ secret('AZURE_DEVOPS_PAT') }}"
                    branch: refs/heads/main
                    templateParameters:
                      environment: staging
                    wait: true
                    timeout: PT30M

                  - id: log_result
                    type: io.kestra.plugin.core.log.Log
                    message: "Run {{ outputs.run_pipeline.id }} ended with {{ outputs.run_pipeline.result }}: {{ outputs.run_pipeline.webUrl }}"
                """
        )
    }
)
@Schema(
    title = "Run an Azure DevOps pipeline",
    description = """
        Triggers a new execution of an Azure DevOps pipeline, optionally waiting for its completion
        and canceling the remote run if the Kestra execution is killed.
        """
)
public class Run extends AbstractAzureDevOpsTask implements RunnableTask<Run.Output> {
    private static final Duration DEFAULT_POLL_INTERVAL = Duration.ofSeconds(5);
    private static final Duration DEFAULT_TIMEOUT = Duration.ofMinutes(30);

    @Schema(
        title = "Pipeline ID",
        description = """
            The ID of the Azure DevOps pipeline to run.
            """
    )
    @NotNull
    @PluginProperty(group = "main")
    protected Property<Integer> pipelineId;

    @Schema(
        title = "Branch or reference",
        description = """
            The git branch or reference name to build (e.g. `main` or `refs/heads/main`).
            """
    )
    @PluginProperty(group = "main")
    protected Property<String> branch;

    @Schema(
        title = "Template parameters",
        description = """
            Key-value pairs matching parameters defined in the pipeline YAML definition.
            """
    )
    @PluginProperty(group = "main")
    protected Property<Map<String, Object>> templateParameters;

    @Schema(
        title = "Pipeline variables",
        description = """
            Variables passed to the pipeline run. Values can be primitive strings or maps containing a `value` key.
            """
    )
    @PluginProperty(group = "main")
    protected Property<Map<String, Object>> variables;

    @Schema(
        title = "Wait for completion",
        description = """
            Whether to block and poll until the pipeline run finishes. Defaults to `false`.
            """
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    protected Property<Boolean> wait = Property.ofValue(false);

    @Schema(
        title = "Poll interval",
        description = """
            Interval between status polling requests when `wait` is enabled. Defaults to `PT5S`.
            """
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    protected Property<Duration> pollInterval = Property.ofValue(DEFAULT_POLL_INTERVAL);

    @Schema(
        title = "Timeout",
        description = """
            Maximum duration to wait for the pipeline run to reach a terminal state. Defaults to `PT30M`.
            """
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<Duration> timeout = Property.ofValue(DEFAULT_TIMEOUT);

    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @Builder.Default
    private final CancellableOperation lifecycle = new CancellableOperation("Azure DevOps pipeline run");

    @Override
    public void kill() {
        this.lifecycle.kill();
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        Integer pId = runContext.render(this.pipelineId).as(Integer.class).orElseThrow();
        Boolean shouldWait = runContext.render(this.wait).as(Boolean.class).orElse(false);
        Duration poll = runContext.render(this.pollInterval).as(Duration.class).orElse(DEFAULT_POLL_INTERVAL);
        Duration maxTimeout = runContext.render(this.timeout).as(Duration.class).orElse(DEFAULT_TIMEOUT);

        String projectApi = buildProjectApiUrl(runContext);
        URI runUri = URI.create("%s/pipelines/%d/runs?api-version=%s".formatted(projectApi, pId, API_VERSION));

        Map<String, Object> payload = buildRunPayload(runContext);

        runContext.logger().info("Triggering Azure DevOps pipeline {}", pId);
        HttpResponse<String> response = executeRequest(runContext, "POST", runUri, payload);

        PipelineRun pipelineRun = parseRunResponse(response.getBody());
        Integer runId = pipelineRun.getId();
        runContext.logger().info("Successfully created Azure DevOps pipeline run {}", runId);

        // Arm cancellation lifecycle
        this.lifecycle.arm(() ->
        {
            try {
                URI cancelUri = URI.create("%s/build/builds/%d?api-version=%s".formatted(projectApi, runId, API_VERSION));
                executeRequest(runContext, "PATCH", cancelUri, Map.of("status", "Cancelling"));
                runContext.logger().info("Dispatched cancellation for Azure DevOps pipeline run {}", runId);
            } catch (Exception e) {
                runContext.logger().warn("Failed to cancel Azure DevOps pipeline run {}", runId, e);
            }
        });

        if (Boolean.TRUE.equals(shouldWait)) {
            pipelineRun = waitForCompletion(runContext, projectApi, pId, runId, poll, maxTimeout);

            if (pipelineRun.getResult() != null && !"succeeded".equalsIgnoreCase(pipelineRun.getResult())) {
                throw new IllegalStateException(
                    "Azure DevOps pipeline run %d completed with non-successful result: %s".formatted(
                        runId,
                        pipelineRun.getResult()
                    )
                );
            }
        }

        return Output.builder()
            .run(pipelineRun)
            .id(pipelineRun.getId())
            .name(pipelineRun.getName())
            .state(pipelineRun.getState())
            .result(pipelineRun.getResult())
            .webUrl(pipelineRun.getWebUrl())
            .createdDate(pipelineRun.getCreatedDate())
            .finishedDate(pipelineRun.getFinishedDate())
            .build();
    }

    private Map<String, Object> buildRunPayload(RunContext runContext) throws Exception {
        Map<String, Object> payload = new HashMap<>();

        String refBranch = runContext.render(this.branch).as(String.class).orElse(null);
        if (refBranch != null && !refBranch.isBlank()) {
            String fullRef = refBranch.startsWith("refs/") ? refBranch : "refs/heads/" + refBranch;
            payload.put(
                "resources", Map.of(
                    "repositories", Map.of(
                        "self", Map.of("refName", fullRef)
                    )
                )
            );
        }

        Map<String, Object> rTemplateParams = runContext.render(this.templateParameters).asMap(String.class, Object.class);
        if (!rTemplateParams.isEmpty()) {
            payload.put("templateParameters", rTemplateParams);
        }

        Map<String, Object> rVariables = runContext.render(this.variables).asMap(String.class, Object.class);
        if (!rVariables.isEmpty()) {
            Map<String, Object> formattedVars = new HashMap<>();
            for (Map.Entry<String, Object> entry : rVariables.entrySet()) {
                if (entry.getValue() instanceof Map) {
                    formattedVars.put(entry.getKey(), entry.getValue());
                } else {
                    formattedVars.put(entry.getKey(), Map.of("value", entry.getValue() == null ? "" : entry.getValue().toString()));
                }
            }
            payload.put("variables", formattedVars);
        }

        return payload;
    }

    private PipelineRun waitForCompletion(
        RunContext runContext,
        String projectApi,
        Integer pipelineId,
        Integer runId,
        Duration pollInterval,
        Duration timeout) throws Exception {
        URI getUri = URI.create("%s/pipelines/%d/runs/%d?api-version=%s".formatted(projectApi, pipelineId, runId, API_VERSION));
        long deadline = System.currentTimeMillis() + timeout.toMillis();

        while (true) {
            HttpResponse<String> response = executeRequest(runContext, "GET", getUri, null);
            PipelineRun run = parseRunResponse(response.getBody());

            String state = run.getState();
            if (state != null && ("completed".equalsIgnoreCase(state) || "canceled".equalsIgnoreCase(state))) {
                runContext.logger().info("Azure DevOps pipeline run {} reached terminal state: {}", runId, state);
                return run;
            }

            if (System.currentTimeMillis() > deadline) {
                this.lifecycle.kill();
                throw new IllegalStateException(
                    "Azure DevOps pipeline run %d did not complete within timeout (%s). Last state: %s".formatted(
                        runId,
                        timeout,
                        state
                    )
                );
            }

            try {
                Thread.sleep(pollInterval.toMillis());
            } catch (InterruptedException e) {
                this.lifecycle.kill();
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for Azure DevOps pipeline run " + runId, e);
            }
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Pipeline run",
            description = """
                The created or completed pipeline run record.
                """
        )
        private final PipelineRun run;

        @Schema(
            title = "Run ID",
            description = """
                The unique identifier of the pipeline run.
                """
        )
        private final Integer id;

        @Schema(
            title = "Run name",
            description = """
                The name or build number of the pipeline run.
                """
        )
        private final String name;

        @Schema(
            title = "Run state",
            description = """
                The state of the pipeline run (e.g. inProgress, completed, canceling, notStarted).
                """
        )
        private final String state;

        @Schema(
            title = "Run result",
            description = """
                The outcome result of the pipeline run (e.g. succeeded, failed, canceled).
                """
        )
        private final String result;

        @Schema(
            title = "Web URL",
            description = """
                Azure DevOps browser URL to view the pipeline run execution.
                """
        )
        private final String webUrl;

        @Schema(
            title = "Creation timestamp",
            description = """
                Timestamp when the pipeline run was created.
                """
        )
        private final Instant createdDate;

        @Schema(
            title = "Finish timestamp",
            description = """
                Timestamp when the pipeline run finished executing.
                """
        )
        private final Instant finishedDate;
    }
}
