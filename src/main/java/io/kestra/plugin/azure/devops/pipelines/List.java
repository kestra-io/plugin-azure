package io.kestra.plugin.azure.devops.pipelines;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;

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
            title = "List all pipeline definitions in an Azure DevOps project",
            full = true,
            code = """
                id: list_pipelines
                namespace: company.team

                tasks:
                  - id: list
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
        Lists pipeline definitions within an Azure DevOps project.
        """
)
public class List extends AbstractAzureDevOpsTask implements RunnableTask<List.Output> {
    @Schema(
        title = "Maximum pipelines to return",
        description = """
            The maximum number of pipelines to return (between 1 and 1000).
            """
    )
    @PluginProperty(group = "advanced")
    private Property<Integer> top;

    @Schema(
        title = "Order by expression",
        description = """
            A sort expression, such as `name asc`.
            """
    )
    @PluginProperty(group = "advanced")
    private Property<String> orderBy;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rawTop = runContext.render(this.top).as(Integer.class).orElse(null);
        var rTop = rawTop != null ? Math.max(1, Math.min(1000, rawTop)) : null;
        var rOrderBy = runContext.render(this.orderBy).as(String.class).orElse(null);

        var projectApi = buildProjectApiUrl(runContext);
        var query = new StringBuilder("?api-version=").append(API_VERSION);
        if (rTop != null) {
            query.append("&$top=").append(rTop);
        }
        if (rOrderBy != null && !rOrderBy.isBlank()) {
            var encodedOrderBy = URLEncoder.encode(rOrderBy, StandardCharsets.UTF_8).replace("+", "%20");
            query.append("&orderBy=").append(encodedOrderBy);
        }

        var uri = URI.create(projectApi + "/pipelines" + query);
        runContext.logger().info("Listing Azure DevOps pipeline definitions");
        var response = executeRequest(runContext, "GET", uri, null);

        if (response.getBody() == null || response.getBody().isBlank()) {
            return Output.builder().pipelines(Collections.emptyList()).total(0).build();
        }

        var root = JacksonMapper.ofJson().readTree(response.getBody());
        var pipelines = new ArrayList<Pipeline>();
        if (root.hasNonNull("value") && root.get("value").isArray()) {
            for (var item : root.get("value")) {
                var p = Pipeline.fromJson(item);
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
                The list of pipeline definitions retrieved.
                """
        )
        private final java.util.List<Pipeline> pipelines;

        @Schema(
            title = "Total pipelines",
            description = """
                The number of pipelines returned.
                """
        )
        private final Integer total;
    }
}
