package io.kestra.plugin.azure.devops.pipelines;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;

import io.kestra.core.http.HttpResponse;
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
            title = "List completed runs of an Azure DevOps pipeline",
            full = true,
            code = """
                id: azure_devops_list_runs
                namespace: company.team

                tasks:
                  - id: list_runs
                    type: io.kestra.plugin.azure.devops.pipelines.ListRuns
                    organization: my-org
                    project: my-project
                    pipelineId: 42
                    personalAccessToken: "{{ secret('AZURE_DEVOPS_PAT') }}"
                    state: completed
                    result: succeeded
                """
        )
    }
)
@Schema(
    title = "List Azure DevOps pipeline runs",
    description = """
        Lists runs of an Azure DevOps pipeline, with optional filtering by state and outcome result.
        """
)
public class ListRuns extends AbstractAzureDevOpsTask implements RunnableTask<ListRuns.Output> {
    @Schema(
        title = "Pipeline ID",
        description = """
            The ID of the pipeline to query runs for.
            """
    )
    @NotNull
    @PluginProperty(group = "main")
    protected Property<Integer> pipelineId;

    @Schema(
        title = "Run state filter",
        description = """
            Filter runs by state (e.g. inProgress, completed, canceling, notStarted).
            """
    )
    @PluginProperty(group = "main")
    protected Property<String> state;

    @Schema(
        title = "Run result filter",
        description = """
            Filter runs by result outcome (e.g. succeeded, failed, canceled).
            """
    )
    @PluginProperty(group = "main")
    protected Property<String> result;

    @Schema(
        title = "Maximum runs to return",
        description = """
            Maximum number of runs to return in outputs. Defaults to 50.
            """
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    protected Property<Integer> maxRuns = Property.ofValue(50);

    @Override
    public Output run(RunContext runContext) throws Exception {
        Integer pId = runContext.render(this.pipelineId).as(Integer.class).orElseThrow();
        String filterState = runContext.render(this.state).as(String.class).orElse(null);
        String filterResult = runContext.render(this.result).as(String.class).orElse(null);
        Integer limit = runContext.render(this.maxRuns).as(Integer.class).orElse(50);

        String projectApi = buildProjectApiUrl(runContext);
        URI uri = URI.create("%s/pipelines/%d/runs?api-version=%s".formatted(projectApi, pId, API_VERSION));

        runContext.logger().info("Listing Azure DevOps pipeline runs for pipeline {}", pId);
        HttpResponse<String> response = executeRequest(runContext, "GET", uri, null);

        JsonNode root = JacksonMapper.ofJson().readTree(response.getBody());
        JsonNode valueNode = root.get("value");

        if (valueNode == null || !valueNode.isArray()) {
            return Output.builder().runs(Collections.emptyList()).total(0).build();
        }

        List<PipelineRun> runs = new ArrayList<>();
        for (JsonNode item : valueNode) {
            PipelineRun runRecord = PipelineRun.fromJson(item);
            if (runRecord == null) {
                continue;
            }

            if (filterState != null && (runRecord.getState() == null || !filterState.equalsIgnoreCase(runRecord.getState()))) {
                continue;
            }

            if (filterResult != null && (runRecord.getResult() == null || !filterResult.equalsIgnoreCase(runRecord.getResult()))) {
                continue;
            }

            runs.add(runRecord);
            if (runs.size() >= limit) {
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
                The list of matching Azure DevOps pipeline run records.
                """
        )
        private final List<PipelineRun> runs;

        @Schema(
            title = "Total runs",
            description = """
                The number of matching pipeline runs returned.
                """
        )
        private final Integer total;
    }
}
