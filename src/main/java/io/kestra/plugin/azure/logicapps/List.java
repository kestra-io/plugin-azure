package io.kestra.plugin.azure.logicapps;

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
                id: azure_logic_apps_list
                namespace: company.team

                tasks:
                  - id: list_workflows
                    type: io.kestra.plugin.azure.logicapps.List
                    tenantId: "{{ secret('AZURE_TENANT_ID') }}"
                    clientId: "{{ secret('AZURE_CLIENT_ID') }}"
                    clientSecret: "{{ secret('AZURE_CLIENT_SECRET') }}"
                    subscriptionId: "{{ secret('AZURE_SUBSCRIPTION_ID') }}"
                    resourceGroupName: my-resource-group
                """
        )
    }
)
@Schema(title = "List Azure Logic App workflows", description = "Lists Logic App workflows in an Azure resource group.")
public class List extends AbstractLogicAppsTask implements RunnableTask<List.Output> {
    @Schema(title = "Maximum workflows", description = "Maximum number of workflows to return. Defaults to 100.")
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<Integer> maxWorkflows = Property.ofValue(100);

    @Override
    public Output run(RunContext runContext) throws Exception {
        String rResourceGroup = runContext.render(this.resourceGroupName).as(String.class).orElseThrow();
        Integer rMaxWorkflows = runContext.render(this.maxWorkflows).as(Integer.class).orElse(100);

        runContext.logger().info("Listing Logic App workflows in resource group '{}'", rResourceGroup);

        try {
            java.util.List<WorkflowRecord> workflows = logicManager(runContext)
                .workflows()
                .listByResourceGroup(rResourceGroup)
                .stream()
                .limit(rMaxWorkflows)
                .map(WorkflowRecord::of)
                .toList();

            return Output.builder()
                .workflows(workflows)
                .total(workflows.size())
                .build();
        } catch (Exception e) {
            throw new Exception(
                "Failed to list Logic App workflows in resource group '" + rResourceGroup + "'",
                e
            );
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Workflows", description = "Logic App workflows returned by Azure.")
        private final java.util.List<WorkflowRecord> workflows;

        @Schema(title = "Total workflows", description = "Number of workflows returned.")
        private final Integer total;
    }
}
