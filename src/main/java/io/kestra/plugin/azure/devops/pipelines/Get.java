package io.kestra.plugin.azure.devops.pipelines;

import java.net.URI;

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
            title = "Get an Azure DevOps pipeline definition by ID using a Personal Access Token",
            full = true,
            code = """
                id: azure_devops_get_pipeline
                namespace: company.team

                tasks:
                  - id: get_pipeline
                    type: io.kestra.plugin.azure.devops.pipelines.Get
                    organization: my-org
                    project: my-project
                    personalAccessToken: "{{ secret('AZURE_DEVOPS_PAT') }}"
                    pipelineId: 42
                """
        )
    }
)
@Schema(
    title = "Get an Azure DevOps pipeline definition",
    description = """
        Retrieves the metadata and configuration of a specific pipeline definition by ID from Azure DevOps.
        """
)
public class Get extends AbstractAzureDevOpsTask implements RunnableTask<Get.Output> {
    @NotNull
    @Schema(
        title = "Pipeline ID",
        description = """
            The unique identifier of the pipeline definition to retrieve.
            """
    )
    @PluginProperty(group = "main")
    private Property<Integer> pipelineId;

    @Schema(
        title = "Pipeline version",
        description = """
            Optional pipeline version/revision to retrieve.
            """
    )
    @PluginProperty(group = "advanced")
    private Property<Integer> pipelineVersion;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Integer rPipelineId = runContext.render(this.pipelineId).as(Integer.class).orElseThrow();
        Integer rVersion = runContext.render(this.pipelineVersion).as(Integer.class).orElse(null);

        String projectApi = buildProjectApiUrl(runContext);
        String url = "%s/pipelines/%d?api-version=%s".formatted(projectApi, rPipelineId, API_VERSION);
        if (rVersion != null) {
            url += "&pipelineVersion=" + rVersion;
        }

        runContext.logger().info("Fetching Azure DevOps pipeline definition {}", rPipelineId);
        HttpResponse<String> response = executeRequest(runContext, "GET", URI.create(url), null);

        JsonNode root = JacksonMapper.ofJson().readTree(response.getBody());
        Pipeline pipeline = Pipeline.fromJson(root);

        return Output.builder()
            .pipeline(pipeline)
            .id(pipeline != null ? pipeline.getId() : null)
            .name(pipeline != null ? pipeline.getName() : null)
            .folder(pipeline != null ? pipeline.getFolder() : null)
            .revision(pipeline != null ? pipeline.getRevision() : null)
            .webUrl(pipeline != null ? pipeline.getWebUrl() : null)
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Pipeline definition",
            description = """
                The retrieved Azure DevOps pipeline definition metadata.
                """
        )
        private final Pipeline pipeline;

        @Schema(
            title = "Pipeline ID",
            description = """
                The unique identifier of the pipeline definition.
                """
        )
        private final Integer id;

        @Schema(
            title = "Pipeline name",
            description = """
                The name of the pipeline definition.
                """
        )
        private final String name;

        @Schema(
            title = "Folder",
            description = """
                The folder containing the pipeline definition.
                """
        )
        private final String folder;

        @Schema(
            title = "Revision number",
            description = """
                The revision number of the pipeline definition.
                """
        )
        private final Integer revision;

        @Schema(
            title = "Web URL",
            description = """
                Azure DevOps browser URL to view the pipeline definition.
                """
        )
        private final String webUrl;
    }
}
