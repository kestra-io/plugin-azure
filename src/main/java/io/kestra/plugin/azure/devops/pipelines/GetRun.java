package io.kestra.plugin.azure.devops.pipelines;

import java.net.URI;
import java.time.Instant;

import io.kestra.core.http.HttpResponse;
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
            title = "Read the state and outcome of an Azure DevOps pipeline run",
            full = true,
            code = """
                id: azure_devops_get_run
                namespace: company.team

                inputs:
                  - id: run_id
                    type: INT

                tasks:
                  - id: get_run
                    type: io.kestra.plugin.azure.devops.pipelines.GetRun
                    organization: my-org
                    project: my-project
                    pipelineId: 42
                    runId: "{{ inputs.run_id }}"
                    personalAccessToken: "{{ secret('AZURE_DEVOPS_PAT') }}"

                  - id: log_state
                    type: io.kestra.plugin.core.log.Log
                    message: "State {{ outputs.get_run.state }}, result {{ outputs.get_run.result }}"
                """
        )
    }
)
@Schema(
    title = "Get an Azure DevOps pipeline run",
    description = """
        Retrieves the current state, outcome result, and URLs of a specific pipeline run in Azure DevOps.
        """
)
public class GetRun extends AbstractAzureDevOpsTask implements RunnableTask<GetRun.Output> {
    @Schema(
        title = "Pipeline ID",
        description = """
            The ID of the pipeline that owns the run.
            """
    )
    @NotNull
    @PluginProperty(group = "main")
    protected Property<Integer> pipelineId;

    @Schema(
        title = "Run ID",
        description = """
            The unique identifier of the pipeline run to retrieve.
            """
    )
    @NotNull
    @PluginProperty(group = "main")
    protected Property<Integer> runId;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Integer pId = runContext.render(this.pipelineId).as(Integer.class).orElseThrow();
        Integer rId = runContext.render(this.runId).as(Integer.class).orElseThrow();

        String projectApi = buildProjectApiUrl(runContext);
        URI uri = URI.create("%s/pipelines/%d/runs/%d?api-version=%s".formatted(projectApi, pId, rId, API_VERSION));

        runContext.logger().info("Fetching Azure DevOps pipeline run {} for pipeline {}", rId, pId);
        HttpResponse<String> response = executeRequest(runContext, "GET", uri, null);

        PipelineRun pipelineRun = parseRunResponse(response.getBody());

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

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Pipeline run",
            description = """
                The retrieved pipeline run record.
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
                The outcome of the pipeline run (e.g. succeeded, failed, canceled).
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
