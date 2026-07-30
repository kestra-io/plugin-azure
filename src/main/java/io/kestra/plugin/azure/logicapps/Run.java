package io.kestra.plugin.azure.logicapps;

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
        String resourceGroup = runContext.render(this.resourceGroupName).as(String.class).orElseThrow();
        String workflow = runContext.render(this.workflowName).as(String.class).orElseThrow();
        String trigger = runContext.render(this.triggerName).as(String.class).orElseThrow();

        runContext.logger().info("Triggering Logic App workflow '{}' trigger '{}'", workflow, trigger);
        Response<Void> response = manager.workflowTriggers().runWithResponse(resourceGroup, workflow, trigger, Context.NONE);

        return Output.builder()
            .workflowName(workflow)
            .triggerName(trigger)
            .statusCode(response.getStatusCode())
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Workflow name", description = "Name of the triggered Logic App workflow.")
        private final String workflowName;

        @Schema(title = "Trigger name", description = "Name of the workflow trigger that was run.")
        private final String triggerName;

        @Schema(title = "HTTP status code", description = "Status code returned by the Azure Logic Apps run trigger operation.")
        private final Integer statusCode;
    }
}
