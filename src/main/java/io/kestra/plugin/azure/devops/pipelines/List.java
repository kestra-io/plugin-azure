package io.kestra.plugin.azure.devops.pipelines;

import java.net.URI;
import java.util.ArrayList;

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
            title = "List pipeline definitions in an Azure DevOps project",
            full = true,
            code = """
                id: azure_devops_list_pipelines
                namespace: company.team

                tasks:
                  - id: list_pipelines
                    type: io.kestra.plugin.azure.devops.pipelines.List
                    organization: my-org
                    project: my-project
                    personalAccessToken: "{{ secret('AZURE_DEVOPS_PAT') }}"
                """
        )
    }
)
@Schema(
    title = "List Azure DevOps pipeline definitions",
    description = """
        Retrieves a list of pipeline definitions for a given Azure DevOps project.
        """
)
public class List extends AbstractAzureDevOpsTask implements RunnableTask<List.Output> {
    @Schema(
        title = "Maximum pipelines to return",
        description = """
            Maximum number of pipeline definitions to return ($top parameter).
            """
    )
    @PluginProperty(group = "advanced")
    private Property<Integer> top;

    @Schema(
        title = "Order by expression",
        description = """
            A sort expression, such as 'name asc'.
            """
    )
    @PluginProperty(group = "advanced")
    private Property<String> orderBy;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Integer rTop = runContext.render(this.top).as(Integer.class).orElse(null);
        String rOrderBy = runContext.render(this.orderBy).as(String.class).orElse(null);

        String projectApi = buildProjectApiUrl(runContext);
        StringBuilder query = new StringBuilder("?api-version=").append(API_VERSION);
        if (rTop != null) {
            query.append("&$top=").append(rTop);
        }
        if (rOrderBy != null && !rOrderBy.isBlank()) {
            query.append("&orderBy=").append(rOrderBy);
        }

        URI uri = URI.create(projectApi + "/pipelines" + query);
        runContext.logger().info("Listing Azure DevOps pipeline definitions");
        HttpResponse<String> response = executeRequest(runContext, "GET", uri, null);

        JsonNode root = JacksonMapper.ofJson().readTree(response.getBody());

        java.util.List<Pipeline> pipelines = new ArrayList<>();
        if (root.hasNonNull("value") && root.get("value").isArray()) {
            for (JsonNode item : root.get("value")) {
                Pipeline p = Pipeline.fromJson(item);
                if (p != null) {
                    pipelines.add(p);
                }
            }
        }

        return Output.builder()
            .pipelines(pipelines)
            .total(pipelines.size())
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Pipelines",
            description = """
                List of retrieved Azure DevOps pipeline definitions.
                """
        )
        private final java.util.List<Pipeline> pipelines;

        @Schema(
            title = "Total pipelines",
            description = """
                Number of pipeline definitions returned.
                """
        )
        private final Integer total;
    }
}
