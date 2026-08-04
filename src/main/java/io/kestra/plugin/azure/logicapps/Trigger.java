package io.kestra.plugin.azure.logicapps;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.stream.Stream;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.Rethrow;
import io.kestra.plugin.azure.shared.AzureIdentityConnectionInterface;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import static io.kestra.core.models.triggers.StatefulTriggerService.*;

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
                id: azure_logic_apps_on_complete
                namespace: company.team

                tasks:
                  - id: handle
                    type: io.kestra.plugin.core.log.Log
                    message: "Run {{ trigger.run.name }} completed with status {{ trigger.run.status }}"

                triggers:
                  - id: on_run_complete
                    type: io.kestra.plugin.azure.logicapps.Trigger
                    tenantId: "{{ secret('AZURE_TENANT_ID') }}"
                    clientId: "{{ secret('AZURE_CLIENT_ID') }}"
                    clientSecret: "{{ secret('AZURE_CLIENT_SECRET') }}"
                    subscriptionId: "{{ secret('AZURE_SUBSCRIPTION_ID') }}"
                    resourceGroupName: my-resource-group
                    workflowName: my-logic-app
                    interval: PT5M
                """
        )
    }
)
@Schema(title = "Trigger flows from Azure Logic App workflow runs", description = "Polls a Logic App workflow and starts an execution for newly observed completed or failed runs.")
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Trigger.Output>, StatefulTriggerInterface, AzureIdentityConnectionInterface {
    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    @Schema(title = "Azure tenant ID", description = "Azure Entra tenant ID used for service principal authentication.")
    @PluginProperty(group = "connection")
    protected Property<String> tenantId;

    @Schema(title = "Azure client ID", description = "Client ID of the Azure app registration.")
    @PluginProperty(group = "connection")
    protected Property<String> clientId;

    @Schema(title = "Azure client secret", description = "Client secret of the Azure app registration.")
    @PluginProperty(secret = true, group = "connection")
    protected Property<String> clientSecret;

    @Schema(title = "PEM certificate", description = "PEM certificate content for certificate-based authentication.")
    @PluginProperty(group = "connection")
    protected Property<String> pemCertificate;

    @Schema(title = "Subscription ID", description = "Azure subscription GUID that owns the Logic App workflow.")
    @PluginProperty(group = "connection")
    protected Property<String> subscriptionId;

    @Schema(title = "Resource group name", description = "Azure resource group containing the Logic App workflow.")
    @PluginProperty(group = "main")
    protected Property<String> resourceGroupName;

    @Schema(title = "Workflow name", description = "Name of the Azure Logic App workflow.")
    @PluginProperty(group = "main")
    protected Property<String> workflowName;

    @Schema(title = "Statuses", description = "Workflow run statuses that should trigger executions. Defaults to `Succeeded` and `Failed`.")
    @Builder.Default
    @PluginProperty(group = "main")
    private Property<java.util.List<String>> statuses = Property.ofValue(java.util.List.of("Succeeded", "Failed"));

    @Builder.Default
    @Schema(title = "State change mode", description = "Stateful trigger change mode used for observed workflow runs.")
    private final Property<On> on = Property.ofValue(On.CREATE);

    @Schema(title = "Maximum runs", description = "Maximum number of recent runs to inspect per polling interval. Defaults to 25.")
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<Integer> maxRuns = Property.ofValue(25);

    @Schema(title = "State key", description = "Custom key used to store observed workflow runs.")
    @PluginProperty(group = "advanced")
    private Property<String> stateKey;

    @Schema(title = "State TTL", description = "How long observed workflow run state is retained.")
    @PluginProperty(group = "advanced")
    private Property<Duration> stateTtl;

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        String state = runContext.render(stateKey).as(String.class).orElse(StatefulTriggerService.defaultKey(context.getNamespace(), context.getFlowId(), id));
        Optional<Duration> ttl = runContext.render(stateTtl).as(Duration.class);
        java.util.List<String> watchedStatuses = runContext.render(statuses).asList(String.class);

        ListRuns.Output listedRuns = ListRuns.builder()
            .id(this.id)
            .type(ListRuns.class.getName())
            .tenantId(this.tenantId)
            .clientId(this.clientId)
            .clientSecret(this.clientSecret)
            .pemCertificate(this.pemCertificate)
            .subscriptionId(this.subscriptionId)
            .resourceGroupName(this.resourceGroupName)
            .workflowName(this.workflowName)
            .maxRuns(this.maxRuns)
            .build()
            .run(runContext);

        var previousState = readState(runContext, state, ttl);
        java.util.List<RunRecord> newRuns = listedRuns.getRuns()
            .stream()
            .filter(run -> watchedStatuses.stream().anyMatch(status -> status.equalsIgnoreCase(run.getStatus())))
            .flatMap(Rethrow.throwFunction(run ->
            {
                Instant modifiedAt = Optional.ofNullable(run.getEndTime())
                    .map(java.time.OffsetDateTime::toInstant)
                    .orElseGet(Instant::now);
                var candidate = StatefulTriggerService.Entry.candidate(run.getId(), run.getStatus(), modifiedAt);
                var stateChange = computeAndUpdateState(previousState, candidate, runContext.render(on).as(On.class).orElse(On.CREATE));

                return stateChange.fire() ? Stream.of(run) : Stream.empty();
            }))
            .toList();

        writeState(runContext, state, previousState, ttl);

        if (newRuns.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(
            TriggerService.generateExecution(
                this, conditionContext, context, Output.builder()
                    .runs(newRuns)
                    .run(newRuns.getFirst())
                    .total(newRuns.size())
                    .build()
            )
        );
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "First workflow run", description = "First newly observed workflow run matching the configured statuses.")
        private final RunRecord run;

        @Schema(title = "Workflow runs", description = "Newly observed workflow runs matching the configured statuses.")
        private final java.util.List<RunRecord> runs;

        @Schema(title = "Total runs", description = "Number of newly observed workflow runs.")
        private final Integer total;
    }
}
