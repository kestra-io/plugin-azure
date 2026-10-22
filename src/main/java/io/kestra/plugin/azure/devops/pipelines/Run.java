package io.kestra.plugin.azure.devops.pipelines;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import io.kestra.core.http.client.HttpClientResponseException;
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
            title = "Trigger an Azure DevOps pipeline and wait for completion",
            full = true,
            code = """
                id: run_azure_devops_pipeline
                namespace: company.team

                tasks:
                  - id: trigger_pipeline
                    type: io.kestra.plugin.azure.devops.pipelines.Run
                    organization: my-org
                    project: my-project
                    pipelineId: 42
                    personalAccessToken: "{{ secret('AZURE_DEVOPS_PAT') }}"
                    branch: main
                    templateParameters:
                      environment: production
                    wait: true
                    maxDuration: PT45M
                """
        ),
        @Example(
            title = "Trigger an Azure DevOps pipeline asynchronously (fire-and-forget)",
            full = true,
            code = """
                id: trigger_async_pipeline
                namespace: company.team

                tasks:
                  - id: async_run
                    type: io.kestra.plugin.azure.devops.pipelines.Run
                    organization: my-org
                    project: my-project
                    pipelineId: 42
                    personalAccessToken: "{{ secret('AZURE_DEVOPS_PAT') }}"
                    wait: false
                """
        )
    }
)
@Schema(
    title = "Trigger and monitor an Azure DevOps pipeline run",
    description = """
        Dispatches a pipeline run in Azure DevOps. Optionally blocks and polls until the run reaches a terminal state.
        Supports cancellation on task kill, dynamic template parameters, variables, and branch selection.
        """
)
public class Run extends AbstractAzureDevOpsTask implements RunnableTask<Run.Output> {
    private static final Duration DEFAULT_POLL_INTERVAL = Duration.ofSeconds(5);
    private static final Duration DEFAULT_TIMEOUT = Duration.ofMinutes(30);

    @Schema(
        title = "Pipeline ID",
        description = """
            The unique identifier of the Azure DevOps pipeline to run.
            """
    )
    @NotNull
    @PluginProperty(group = "main")
    protected Property<Integer> pipelineId;

    @Schema(
        title = "Target branch or ref",
        description = """
            Branch name (e.g. `main`, `release/1.0`) or Git reference to build.
            """
    )
    @PluginProperty(group = "main")
    protected Property<String> branch;

    @Schema(
        title = "Template parameters",
        description = """
            Optional template parameters passed to the pipeline run definition.
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
        title = "Maximum duration",
        description = """
            Maximum duration to wait for the pipeline run to reach a terminal state when `wait` is true. Defaults to `PT30M`.
            """
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<Duration> maxDuration = Property.ofValue(DEFAULT_TIMEOUT);

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
        var rPipelineId = runContext.render(this.pipelineId)
            .as(Integer.class)
            .orElseThrow(() -> new IllegalArgumentException("pipelineId is required"));
        var rWait = runContext.render(this.wait).as(Boolean.class).orElse(false);
        var rPollInterval = runContext.render(this.pollInterval).as(Duration.class).orElse(DEFAULT_POLL_INTERVAL);
        var rMaxDuration = runContext.render(this.maxDuration).as(Duration.class).orElse(DEFAULT_TIMEOUT);

        var projectApi = buildProjectApiUrl(runContext);
        var runUri = URI.create("%s/pipelines/%d/runs?api-version=%s".formatted(projectApi, rPipelineId, API_VERSION));
        var authHeader = getAuthorizationHeader(runContext, runUri);

        var payload = buildRunPayload(runContext);

        runContext.logger().info("Triggering Azure DevOps pipeline {}", rPipelineId);
        var response = executeRequest(runContext, authHeader, "POST", runUri, payload);

        var pipelineRun = parseRunResponse(response.getBody(), runUri);
        var runId = pipelineRun.getId();
        runContext.logger().info("Successfully created Azure DevOps pipeline run {}", runId);

        this.lifecycle.arm(() ->
        {
            try {
                var cancelUri = URI.create("%s/build/builds/%d?api-version=%s".formatted(projectApi, runId, API_VERSION));
                executeRequest(runContext, authHeader, "PATCH", cancelUri, Map.of("status", "Cancelling"));
                runContext.logger().info("Dispatched cancellation for Azure DevOps pipeline run {}", runId);
            } catch (Exception e) {
                runContext.logger().warn("Failed to cancel Azure DevOps pipeline run {}", runId, e);
            }
        });

        if (Boolean.TRUE.equals(rWait)) {
            pipelineRun = waitForCompletion(runContext, authHeader, projectApi, rPipelineId, runId, rPollInterval, rMaxDuration);

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
        var payload = new HashMap<String, Object>();

        var refBranch = runContext.render(this.branch).as(String.class).orElse(null);
        if (refBranch != null && !refBranch.isBlank()) {
            var fullRef = refBranch.startsWith("refs/") ? refBranch : "refs/heads/" + refBranch;
            payload.put(
                "resources", Map.of(
                    "repositories", Map.of(
                        "self", Map.of("refName", fullRef)
                    )
                )
            );
        }

        var rTemplateParams = runContext.render(this.templateParameters).asMap(String.class, Object.class);
        if (!rTemplateParams.isEmpty()) {
            payload.put("templateParameters", rTemplateParams);
        }

        var rVariables = runContext.render(this.variables).asMap(String.class, Object.class);
        if (!rVariables.isEmpty()) {
            var formattedVars = new HashMap<String, Object>();
            for (var entry : rVariables.entrySet()) {
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
        String authHeader,
        String projectApi,
        Integer pipelineId,
        Integer runId,
        Duration pollInterval,
        Duration maxDuration) throws Exception {
        var getUri = URI.create("%s/pipelines/%d/runs/%d?api-version=%s".formatted(projectApi, pipelineId, runId, API_VERSION));
        var deadline = System.currentTimeMillis() + maxDuration.toMillis();
        int consecutiveTransientErrors = 0;
        final int maxConsecutiveTransientErrors = 3;

        while (true) {
            PipelineRun run = null;
            try {
                var response = executeRequest(runContext, authHeader, "GET", getUri, null);
                run = parseRunResponse(response.getBody(), getUri);
                consecutiveTransientErrors = 0;
            } catch (Exception e) {
                if (isTransientError(e) && ++consecutiveTransientErrors <= maxConsecutiveTransientErrors) {
                    runContext.logger().warn(
                        "Transient error while polling Azure DevOps pipeline run {} (attempt {}/{}): {}",
                        runId,
                        consecutiveTransientErrors,
                        maxConsecutiveTransientErrors,
                        e.getMessage()
                    );
                } else {
                    this.lifecycle.kill();
                    throw new IllegalStateException(
                        "Failed while polling Azure DevOps pipeline run %d. Dispatched cancellation.".formatted(runId),
                        e
                    );
                }
            }

            if (run != null) {
                var state = run.getState();
                if (state != null && ("completed".equalsIgnoreCase(state) || "canceled".equalsIgnoreCase(state))) {
                    runContext.logger().info("Azure DevOps pipeline run {} reached terminal state: {}", runId, state);
                    return run;
                }
            }

            if (System.currentTimeMillis() > deadline) {
                this.lifecycle.kill();
                throw new IllegalStateException(
                    "Azure DevOps pipeline run %d did not complete within timeout (%s).".formatted(
                        runId,
                        maxDuration
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

    private static boolean isTransientError(Exception e) {
        Throwable cause = e;
        while (cause != null) {
            if (cause instanceof HttpClientResponseException scre) {
                int code = scre.getResponse() != null && scre.getResponse().getStatus() != null ? scre.getResponse().getStatus().getCode() : -1;
                return code == 429 || (code >= 500 && code < 600);
            }
            if (cause instanceof IOException) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
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
