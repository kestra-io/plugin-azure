package io.kestra.plugin.azure.logicapps;

import java.net.URI;

import com.azure.core.http.rest.Response;
import com.azure.core.util.Context;
import com.azure.resourcemanager.logic.LogicManager;

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
            title = "Trigger workflow",
            full = true,
            code = """
                id: azure_logic_apps_run
                namespace: company.team

                tasks:
                  - id: run_workflow
                    type: io.kestra.plugin.azure.logicapps.Run
                    tenantId: "{{ secret('AZURE_TENANT_ID') }}"
                    clientId: "{{ secret('AZURE_CLIENT_ID') }}"
                    clientSecret: "{{ secret('AZURE_CLIENT_SECRET') }}"
                    subscriptionId: "{{ secret('AZURE_SUBSCRIPTION_ID') }}"
                    resourceGroupName: my-resource-group
                    workflowName: my-logic-app
                """
        )
    }
)
@Schema(title = "Trigger an Azure Logic App workflow", description = "Runs a Logic App workflow trigger using Azure service principal credentials.")
public class Run extends AbstractLogicAppsWorkflowTask implements RunnableTask<Run.Output> {
    @Schema(title = "Trigger name", description = "Name of the workflow trigger to run. Defaults to `manual`.")
    @Builder.Default
    @PluginProperty(group = "main")
    private Property<String> triggerName = Property.ofValue("manual");

    @Override
    public Output run(RunContext runContext) throws Exception {
        LogicManager manager = logicManager(runContext);
        String rResourceGroup = runContext.render(this.resourceGroupName).as(String.class).orElseThrow();
        String rWorkflowName = runContext.render(this.workflowName).as(String.class).orElseThrow();
        String rTriggerName = runContext.render(this.triggerName).as(String.class).orElseThrow();

        runContext.logger().info("Triggering Logic App workflow '{}' trigger '{}'", rWorkflowName, rTriggerName);

        return withAzureContext(
            runContext,
            "Failed to trigger Logic App workflow '" + rWorkflowName + "' using trigger '" + rTriggerName + "' in resource group '" + rResourceGroup + "'",
            () ->
            {
                Response<Void> response = manager.workflowTriggers().runWithResponse(rResourceGroup, rWorkflowName, rTriggerName, Context.NONE);
                String location = response.getHeaders().getValue("Location");

                return Output.builder()
                    .workflowName(rWorkflowName)
                    .triggerName(rTriggerName)
                    .runId(location == null ? null : extractRunId(location))
                    .statusCode(response.getStatusCode())
                    .build();
            }
        );
    }

    private static String extractRunId(String location) {
        if (location == null) {
            return null;
        }

        String path = URI.create(location).getPath();
        if (path == null || path.isEmpty()) {
            return null;
        }

        String[] parts = path.split("/");
        for (int i = parts.length - 1; i >= 0; i--) {
            if (parts[i] != null && !parts[i].isEmpty()) {
                return parts[i];
            }
        }

        return null;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Workflow name", description = "Name of the triggered Logic App workflow.")
        private final String workflowName;

        @Schema(title = "Trigger name", description = "Name of the workflow trigger that was run.")
        private final String triggerName;

        @Schema(title = "Run ID", description = "Identifier of the triggered Logic App workflow run, extracted from the response Location header when available.")
        private final String runId;

        @Schema(title = "HTTP status code", description = "Status code returned by the Azure Logic Apps run trigger operation.")
        private final Integer statusCode;
    }
}
