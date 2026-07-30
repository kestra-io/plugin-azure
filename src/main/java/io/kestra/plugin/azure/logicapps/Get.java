package io.kestra.plugin.azure.logicapps;

import com.azure.resourcemanager.logic.models.Workflow;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
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
                id: azure_logic_apps_get
                namespace: company.team

                tasks:
                  - id: get_workflow
                    type: io.kestra.plugin.azure.logicapps.Get
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
@Schema(title = "Get an Azure Logic App workflow", description = "Retrieves metadata for a Logic App workflow.")
public class Get extends AbstractLogicAppsWorkflowTask implements RunnableTask<Get.Output> {
    @Override
    public Output run(RunContext runContext) throws Exception {
        String resourceGroup = runContext.render(this.resourceGroupName).as(String.class).orElseThrow();
        String workflowName = runContext.render(this.workflowName).as(String.class).orElseThrow();

        runContext.logger().info("Fetching Logic App workflow '{}'", workflowName);
        Workflow workflow = logicManager(runContext).workflows().getByResourceGroup(resourceGroup, workflowName);

        return Output.builder()
            .workflow(WorkflowRecord.of(workflow))
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Workflow", description = "Logic App workflow metadata returned by Azure.")
        private final WorkflowRecord workflow;
    }
}
