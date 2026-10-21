package io.kestra.plugin.azure.devops.pipelines;

import java.net.URI;
import java.util.Map;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
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
            title = "Cancel an in-progress Azure DevOps pipeline run",
            full = true,
            code = """
                id: azure_devops_cancel_run
                namespace: company.team

                tasks:
                  - id: cancel_run
                    type: io.kestra.plugin.azure.devops.pipelines.Cancel
                    organization: my-org
                    project: my-project
                    runId: 101
                    personalAccessToken: "{{ secret('AZURE_DEVOPS_PAT') }}"
                """
        )
    }
)
@Schema(
    title = "Cancel an Azure DevOps pipeline run",
    description = """
        Sends a cancellation request for an active, in-progress Azure DevOps pipeline run.
        """
)
public class Cancel extends AbstractAzureDevOpsTask implements RunnableTask<VoidOutput> {
    @Schema(
        title = "Run ID",
        description = """
            The unique identifier of the pipeline run (or build) to cancel.
            """
    )
    @NotNull
    @PluginProperty(group = "main")
    protected Property<Integer> runId;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        var rRunId = runContext.render(this.runId)
            .as(Integer.class)
            .orElseThrow(() -> new IllegalArgumentException("runId is required"));

        var projectApi = buildProjectApiUrl(runContext);
        var uri = URI.create("%s/build/builds/%d?api-version=%s".formatted(projectApi, rRunId, API_VERSION));

        runContext.logger().info("Cancelling Azure DevOps pipeline run {}", rRunId);
        executeRequest(runContext, "PATCH", uri, Map.of("status", "Cancelling"));

        return null;
    }
}
