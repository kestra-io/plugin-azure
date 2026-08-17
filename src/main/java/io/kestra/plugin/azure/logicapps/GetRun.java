package io.kestra.plugin.azure.logicapps;

import com.azure.resourcemanager.logic.models.WorkflowRun;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            title = "Get run",
            full = true,
            code = """
                id: azure_logic_apps_get_run
                namespace: company.team

                tasks:
                  - id: get_run
                    type: io.kestra.plugin.azure.logicapps.GetRun
                    tenantId: "{{ secret('AZURE_TENANT_ID') }}"
                    clientId: "{{ secret('AZURE_CLIENT_ID') }}"
                    clientSecret: "{{ secret('AZURE_CLIENT_SECRET') }}"
                    subscriptionId: "{{ secret('AZURE_SUBSCRIPTION_ID') }}"
                    resourceGroupName: my-resource-group
                    workflowName: my-logic-app
                    runId: "08585287571846573488044702123CU00"
                """
        )
    }
)
@Schema(title = "Get an Azure Logic App workflow run", description = "Retrieves status, timings, error details, and outputs for a Logic App workflow run.")
public class GetRun extends AbstractLogicAppsWorkflowTask implements RunnableTask<GetRun.Output> {
    @Schema(title = "Run ID", description = "Name or ID of the workflow run to retrieve.")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> runId;

    @Override
    public Output run(RunContext runContext) throws Exception {
        String rResourceGroup = runContext.render(this.resourceGroupName).as(String.class).orElseThrow();
        String rWorkflowName = runContext.render(this.workflowName).as(String.class).orElseThrow();
        String rRunId = runContext.render(this.runId).as(String.class).orElseThrow();

        runContext.logger().info("Fetching Logic App workflow '{}' run '{}'", rWorkflowName, rRunId);
        return withAzureContext(
            runContext,
            "Failed to retrieve Logic App workflow run '" + rRunId + "' for workflow '" + rWorkflowName + "' in resource group '" + rResourceGroup + "'",
            () ->
            {
                WorkflowRun run = logicManager(runContext).workflowRuns().get(rResourceGroup, rWorkflowName, rRunId);

                return Output.builder()
                    .run(RunRecord.of(run))
                    .build();
            }
        );
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Workflow run", description = "Workflow run details returned by Azure Logic Apps.")
        private final RunRecord run;
    }
}
