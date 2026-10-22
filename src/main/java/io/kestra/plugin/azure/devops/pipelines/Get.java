package io.kestra.plugin.azure.devops.pipelines;

import java.net.URI;

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
            title = "Get an Azure DevOps pipeline definition",
            full = true,
            code = """
                id: get_pipeline
                namespace: company.team

                tasks:
                  - id: get
                    type: io.kestra.plugin.azure.devops.pipelines.Get
                    organization: my-org
                    project: my-project
                    pipelineId: 42
                    personalAccessToken: "{{ secret('AZURE_DEVOPS_PAT') }}"
                """
        )
    }
)
@Schema(
    title = "Get an Azure DevOps pipeline definition",
    description = """
        Retrieves metadata for an Azure DevOps pipeline definition.
        """
)
public class Get extends AbstractAzureDevOpsTask implements RunnableTask<Get.Output> {
    @Schema(
        title = "Pipeline ID",
        description = """
            The unique identifier of the pipeline definition to retrieve.
            """
    )
    @NotNull
    @PluginProperty(group = "main")
    protected Property<Integer> pipelineId;

    @Schema(
        title = "Pipeline version",
        description = """
            Optional version revision of the pipeline definition to retrieve.
            """
    )
    @PluginProperty(group = "advanced")
    protected Property<Integer> pipelineVersion;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rPipelineId = runContext.render(this.pipelineId)
            .as(Integer.class)
            .orElseThrow(() -> new IllegalArgumentException("pipelineId is required"));
        var rVersion = runContext.render(this.pipelineVersion).as(Integer.class).orElse(null);

        var projectApi = buildProjectApiUrl(runContext);
        var url = "%s/pipelines/%d?api-version=%s".formatted(projectApi, rPipelineId, API_VERSION);
        if (rVersion != null) {
            url += "&pipelineVersion=" + rVersion;
        }

        runContext.logger().info("Fetching Azure DevOps pipeline definition {}", rPipelineId);
        var uri = URI.create(url);
        var response = executeRequest(runContext, "GET", uri, null);

        if (response.getBody() == null || response.getBody().isBlank()) {
            throw new IllegalStateException("Empty response from Azure DevOps API for " + uri);
        }

        var root = JacksonMapper.ofJson().readTree(response.getBody());
        var pipeline = Pipeline.fromJson(root);
        if (pipeline == null) {
            throw new IllegalStateException("Failed to parse pipeline definition from Azure DevOps API response");
        }

        return Output.builder()
            .id(pipeline.getId())
            .name(pipeline.getName())
            .folder(pipeline.getFolder())
            .revision(pipeline.getRevision())
            .webUrl(pipeline.getWebUrl())
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
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
