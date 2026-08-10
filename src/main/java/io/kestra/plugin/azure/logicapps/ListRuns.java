package io.kestra.plugin.azure.logicapps;

import java.util.Optional;

import com.azure.core.util.Context;
import com.azure.resourcemanager.logic.models.WorkflowStatus;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
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
            title = "List runs",
            full = true,
            code = """
                id: azure_logic_apps_list_runs
                namespace: company.team

                tasks:
                  - id: list_runs
                    type: io.kestra.plugin.azure.logicapps.ListRuns
                    tenantId: "{{ secret('AZURE_TENANT_ID') }}"
                    clientId: "{{ secret('AZURE_CLIENT_ID') }}"
                    clientSecret: "{{ secret('AZURE_CLIENT_SECRET') }}"
                    subscriptionId: "{{ secret('AZURE_SUBSCRIPTION_ID') }}"
                    resourceGroupName: my-resource-group
                    workflowName: my-logic-app
                    statusFilter: Failed

                  - id: log_count
                    type: io.kestra.plugin.core.log.Log
                    message: "Found {{ outputs.list_runs.total }} failed runs"
                """
        )
    }
)
@Schema(title = "List Azure Logic App workflow runs", description = "Lists recent workflow runs for a Logic App, optionally filtered by run status.")
public class ListRuns extends AbstractLogicAppsWorkflowTask implements RunnableTask<ListRuns.Output> {
    @Schema(title = "Status filter", description = "Optional workflow run status to filter on, for example `Succeeded`, `Failed`, or `Cancelled`.")
    @PluginProperty(group = "main")
    private Property<String> statusFilter;

    @Schema(title = "Maximum runs", description = "Maximum number of workflow runs to return. Defaults to 100.")
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<Integer> maxRuns = Property.ofValue(100);

    @Override
    public Output run(RunContext runContext) throws Exception {
        String rResourceGroup = runContext.render(this.resourceGroupName).as(String.class).orElseThrow();
        String rWorkflowName = runContext.render(this.workflowName).as(String.class).orElseThrow();
        Integer rTop = runContext.render(this.maxRuns).as(Integer.class).orElse(100);
        String rFilter = runContext.render(this.statusFilter).as(String.class).map(ListRuns::statusFilter).orElse(null);

        runContext.logger().info("Listing Logic App workflow '{}' runs", rWorkflowName);
        return withAzureContext(
            runContext,
            "Failed to list Logic App workflow runs for workflow '" + rWorkflowName + "' in resource group '" + rResourceGroup + "'",
            () ->
            {
                java.util.List<RunRecord> runs = logicManager(runContext)
                    .workflowRuns()
                    .list(rResourceGroup, rWorkflowName, rTop, rFilter, Context.NONE)
                    .stream()
                    .map(RunRecord::of)
                    .toList();

                return Output.builder()
                    .runs(runs)
                    .total(runs.size())
                    .build();
            }
        );
    }

    static String statusFilter(String status) {
        return Optional.ofNullable(status)
            .map(WorkflowStatus::fromString)
            .map(WorkflowStatus::toString)
            .map(value -> "status eq '" + value.replace("'", "''") + "'")
            .orElse(null);
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Workflow runs", description = "Workflow runs returned by Azure Logic Apps.")
        private final java.util.List<RunRecord> runs;

        @Schema(title = "Total runs", description = "Number of workflow runs returned.")
        private final Integer total;
    }
}
