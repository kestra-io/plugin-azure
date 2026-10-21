package io.kestra.plugin.azure.devops.pipelines;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;

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
            title = "List recent completed runs for an Azure DevOps pipeline",
            full = true,
            code = """
                id: list_pipeline_runs
                namespace: company.team

                tasks:
                  - id: list_runs
                    type: io.kestra.plugin.azure.devops.pipelines.ListRuns
                    organization: my-org
                    project: my-project
                    pipelineId: 42
                    personalAccessToken: "{{ secret('AZURE_DEVOPS_PAT') }}"
                    state: completed
                    maxRuns: 20
                """
        )
    }
)
@Schema(
    title = "List Azure DevOps pipeline runs",
    description = """
        Queries recent pipeline runs or builds for an Azure DevOps pipeline.
        Supports server-side filtering by state, result, and top N limit.
        """
)
public class ListRuns extends AbstractAzureDevOpsTask implements RunnableTask<ListRuns.Output> {
    @Schema(
        title = "Pipeline ID",
        description = """
            The unique identifier of the pipeline whose runs to retrieve.
            """
    )
    @NotNull
    @PluginProperty(group = "main")
    protected Property<Integer> pipelineId;

    @Schema(
        title = "Run state filter",
        description = """
            Filter runs by state (e.g. `completed`, `inProgress`, `notStarted`, `cancelling`).
            """
    )
    @PluginProperty(group = "main")
    protected Property<String> state;

    @Schema(
        title = "Run result filter",
        description = """
            Filter runs by result outcome (e.g. `succeeded`, `failed`, `canceled`).
            """
    )
    @PluginProperty(group = "main")
    protected Property<String> result;

    @Schema(
        title = "Maximum runs to return",
        description = """
            Maximum number of runs to return in outputs (between 1 and 1000). Defaults to 50.
            """
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    protected Property<Integer> maxRuns = Property.ofValue(50);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rPipelineId = runContext.render(this.pipelineId)
            .as(Integer.class)
            .orElseThrow(() -> new IllegalArgumentException("pipelineId is required"));
        var rState = runContext.render(this.state).as(String.class).orElse(null);
        var rResult = runContext.render(this.result).as(String.class).orElse(null);
        var rawLimit = runContext.render(this.maxRuns).as(Integer.class).orElse(50);
        var rLimit = Math.max(1, Math.min(1000, rawLimit));

        var projectApi = buildProjectApiUrl(runContext);
        var uriBuilder = new StringBuilder(projectApi)
            .append("/build/builds?definitions=").append(rPipelineId)
            .append("&queryOrder=finishTimeDescending")
            .append("&$top=").append(rLimit)
            .append("&api-version=").append(API_VERSION);

        if (rState != null && !rState.isBlank()) {
            uriBuilder.append("&statusFilter=").append(URLEncoder.encode(rState, StandardCharsets.UTF_8).replace("+", "%20"));
        }
        if (rResult != null && !rResult.isBlank()) {
            uriBuilder.append("&resultFilter=").append(URLEncoder.encode(rResult, StandardCharsets.UTF_8).replace("+", "%20"));
        }

        var uri = URI.create(uriBuilder.toString());

        runContext.logger().info("Listing Azure DevOps pipeline runs for pipeline {}", rPipelineId);
        var response = executeRequest(runContext, "GET", uri, null);

        if (response.getBody() == null || response.getBody().isBlank()) {
            return Output.builder().runs(Collections.emptyList()).total(0).build();
        }

        var root = JacksonMapper.ofJson().readTree(response.getBody());
        var valueNode = root.get("value");

        if (valueNode == null || !valueNode.isArray()) {
            return Output.builder().runs(Collections.emptyList()).total(0).build();
        }

        var runs = new ArrayList<PipelineRun>();
        for (var item : valueNode) {
            var runRecord = PipelineRun.fromJson(item);
            if (runRecord == null) {
                continue;
            }

            if (rState != null && (runRecord.getState() == null || !rState.equalsIgnoreCase(runRecord.getState()))) {
                continue;
            }

            if (rResult != null && (runRecord.getResult() == null || !rResult.equalsIgnoreCase(runRecord.getResult()))) {
                continue;
            }

            runs.add(runRecord);
            if (runs.size() >= rLimit) {
                break;
            }
        }

        return Output.builder()
            .runs(runs)
            .total(runs.size())
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Pipeline runs",
            description = """
                The retrieved pipeline runs.
                """
        )
        private final List<PipelineRun> runs;

        @Schema(
            title = "Total runs",
            description = """
                The total number of runs returned.
                """
        )
        private final Integer total;
    }
}
