package io.kestra.plugin.azure.devops.pipelines;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

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
            title = "Create an Azure DevOps pipeline definition",
            full = true,
            code = """
                id: azure_devops_create_pipeline
                namespace: company.team

                tasks:
                  - id: create_pipeline
                    type: io.kestra.plugin.azure.devops.pipelines.Create
                    organization: my-org
                    project: my-project
                    personalAccessToken: "{{ secret('AZURE_DEVOPS_PAT') }}"
                    name: "MyPipeline"
                    folder: '\\'
                    configuration:
                      type: "yaml"
                      path: "azure-pipelines.yml"
                      repository:
                        id: "repo-id"
                        type: "azureReposGit"
                """
        )
    }
)
@Schema(
    title = "Create an Azure DevOps pipeline definition",
    description = """
        Creates a new pipeline definition in an Azure DevOps project using the Pipelines REST API v7.1.
        """
)
public class Create extends AbstractAzureDevOpsTask implements RunnableTask<Create.Output> {
    @NotNull
    @Schema(
        title = "Pipeline name",
        description = """
            The name of the new pipeline definition.
            """
    )
    @PluginProperty(group = "main")
    private Property<String> name;

    @Schema(
        title = "Pipeline folder",
        description = """
            The folder of the pipeline definition (e.g. `\\`). Defaults to `\\`.
            """
    )
    @Builder.Default
    @PluginProperty(group = "main")
    private Property<String> folder = Property.ofValue("\\");

    @Schema(
        title = "Configuration parameters",
        description = """
            Pipeline configuration parameters such as configuration type, YAML path, and repository reference.
            """
    )
    @PluginProperty(group = "main")
    private Property<Map<String, Object>> configuration;

    @Override
    public Output run(RunContext runContext) throws Exception {
        String rName = runContext.render(this.name).as(String.class).orElseThrow();
        String rFolder = runContext.render(this.folder).as(String.class).orElse("\\");
        Map<String, Object> rConfiguration = runContext.render(this.configuration).asMap(String.class, Object.class);

        Map<String, Object> payload = new HashMap<>();
        payload.put("name", rName);
        payload.put("folder", rFolder);
        if (rConfiguration != null && !rConfiguration.isEmpty()) {
            payload.put("configuration", rConfiguration);
        }

        String projectApi = buildProjectApiUrl(runContext);
        URI uri = URI.create("%s/pipelines?api-version=%s".formatted(projectApi, API_VERSION));

        runContext.logger().info("Creating Azure DevOps pipeline definition '{}'", rName);
        HttpResponse<String> response = executeRequest(runContext, "POST", uri, payload);

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
                The newly created Azure DevOps pipeline definition.
                """
        )
        private final Pipeline pipeline;

        @Schema(
            title = "Pipeline ID",
            description = """
                The unique identifier of the created pipeline definition.
                """
        )
        private final Integer id;

        @Schema(
            title = "Pipeline name",
            description = """
                The name of the created pipeline definition.
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
                Azure DevOps browser URL to view the created pipeline definition.
                """
        )
        private final String webUrl;
    }
}
