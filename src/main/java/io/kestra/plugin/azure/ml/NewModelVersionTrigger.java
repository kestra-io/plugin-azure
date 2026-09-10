package io.kestra.plugin.azure.ml;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.models.triggers.StatefulTriggerInterface;
import io.kestra.core.models.triggers.StatefulTriggerService;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.models.triggers.TriggerService;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
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
            title = "Trigger a flow whenever a new version of a model is registered.",
            full = true,
            code = """
                id: react_to_new_model_version
                namespace: company.team

                tasks:
                  - id: log
                    type: io.kestra.plugin.core.log.Log
                    message: "New model version: {{ trigger.version }} at {{ trigger.modelUri }}"

                triggers:
                  - id: watch
                    type: io.kestra.plugin.azure.ml.NewModelVersionTrigger
                    interval: PT5M
                    tenantId: "{{ secret('AZURE_TENANT_ID') }}"
                    clientId: "{{ secret('AZURE_CLIENT_ID') }}"
                    clientSecret: "{{ secret('AZURE_CLIENT_SECRET') }}"
                    subscriptionId: "{{ secret('AZURE_SUBSCRIPTION_ID') }}"
                    resourceGroupName: ml-rg
                    workspaceName: ml-workspace
                    modelName: fraud-detector
                """
        )
    }
)
@Schema(
    title = "Trigger a flow when a new Azure Machine Learning model version is registered",
    description = "Polls a model's latest version on a schedule and starts an execution when a new version is detected. The last-seen version is persisted in the flow's namespace KV Store so the trigger does not re-fire for a version it already delivered. Only the latest version at each poll is compared against the last-seen one: if several versions are registered between two polls, only the latest of them fires and the intermediate ones are not individually delivered — the same interval-bound characteristic as every other stateful polling trigger in this plugin."
)
public class NewModelVersionTrigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<NewModelVersionTrigger.Output>, StatefulTriggerInterface {
    @Schema(title = "Azure AD tenant ID (GUID)")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> tenantId;

    @Schema(title = "Client ID of the Azure AD application")
    @PluginProperty(group = "connection")
    private Property<String> clientId;

    @Schema(title = "Client secret for the Azure AD application")
    @PluginProperty(secret = true, group = "connection")
    @ToString.Exclude
    private Property<String> clientSecret;

    @Schema(title = "PEM-encoded certificate content for client authentication")
    @PluginProperty(secret = true, group = "advanced")
    @ToString.Exclude
    private Property<String> pemCertificate;

    @Schema(title = "Subscription ID", description = "Azure subscription GUID that owns the Machine Learning workspace")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> subscriptionId;

    @Schema(title = "Resource group name", description = "Resource group containing the Machine Learning workspace")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> resourceGroupName;

    @Schema(title = "Workspace name", description = "Name of the Azure Machine Learning workspace")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> workspaceName;

    @Schema(title = "Model name", description = "Name of the model to watch for new versions")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> modelName;

    @Builder.Default
    private final Duration interval = Duration.ofMinutes(5);

    @Builder.Default
    private final Property<On> on = Property.ofValue(On.CREATE_OR_UPDATE);

    private Property<String> stateKey;

    private Property<Duration> stateTtl;

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        var rOn = runContext.render(on).as(On.class).orElse(On.CREATE_OR_UPDATE);
        var rStateKey = runContext.render(stateKey).as(String.class).orElse(StatefulTriggerService.defaultKey(context.getNamespace(), context.getFlowId(), this.id));
        var rStateTtl = runContext.render(stateTtl).as(Duration.class);

        GetModel task = GetModel.builder()
            .id(this.id)
            .type(GetModel.class.getName())
            .tenantId(this.tenantId)
            .clientId(this.clientId)
            .clientSecret(this.clientSecret)
            .pemCertificate(this.pemCertificate)
            .subscriptionId(this.subscriptionId)
            .resourceGroupName(this.resourceGroupName)
            .workspaceName(this.workspaceName)
            .modelName(this.modelName)
            .modelVersion(Property.ofValue("latest"))
            .build();

        GetModel.Output latest;
        try {
            latest = task.run(runContext);
        } catch (GetModel.NoModelVersionRegisteredException e) {
            // the model exists but has no version registered yet — not an error, just nothing to fire on
            return Optional.empty();
        }

        var previousState = StatefulTriggerService.readState(runContext, rStateKey, rStateTtl);
        var candidate = StatefulTriggerService.Entry.candidate(latest.getModelName(), latest.getVersion(), Instant.now());
        var stateChange = StatefulTriggerService.computeAndUpdateState(previousState, candidate, rOn);
        StatefulTriggerService.writeState(runContext, rStateKey, previousState, rStateTtl);

        if (!stateChange.fire()) {
            return Optional.empty();
        }

        var output = Output.builder()
            .modelName(latest.getModelName())
            .version(latest.getVersion())
            .modelUri(latest.getModelUri())
            .build();

        return Optional.of(TriggerService.generateExecution(this, conditionContext, context, output));
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Model name")
        private String modelName;

        @Schema(title = "Model version", description = "Newly detected model version")
        private String version;

        @Schema(title = "Model URI", description = "Storage URI backing this model version")
        private java.net.URI modelUri;
    }
}
