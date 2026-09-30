package io.kestra.plugin.azure.ml;

import com.azure.core.management.AzureEnvironment;
import com.azure.core.management.profile.AzureProfile;
import com.azure.resourcemanager.machinelearning.MachineLearningManager;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.shared.AbstractAzureIdentityConnection;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * Base class for Azure Machine Learning tasks, holding the workspace coordinates and the ARM client factory shared
 * by every task in this package.
 */
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractMachineLearningTask extends AbstractAzureIdentityConnection {
    @Schema(title = "Subscription ID", description = "Azure subscription GUID that owns the Machine Learning workspace")
    @NotNull
    @PluginProperty(group = "main")
    protected Property<String> subscriptionId;

    @Schema(title = "Resource group name", description = "Resource group containing the Machine Learning workspace")
    @NotNull
    @PluginProperty(group = "main")
    protected Property<String> resourceGroupName;

    @Schema(title = "Workspace name", description = "Name of the Azure Machine Learning workspace")
    @NotNull
    @PluginProperty(group = "main")
    protected Property<String> workspaceName;

    protected MachineLearningManager machineLearningManager(RunContext runContext) throws IllegalVariableEvaluationException {
        return MachineLearningManager.authenticate(credentials(runContext), profile(runContext));
    }

    protected AzureProfile profile(RunContext runContext) throws IllegalVariableEvaluationException {
        return new AzureProfile(
            runContext.render(this.tenantId).as(String.class).orElse(null),
            runContext.render(this.subscriptionId).as(String.class).orElse(null),
            AzureEnvironment.AZURE
        );
    }

    protected String rSubscriptionId(RunContext runContext) throws IllegalVariableEvaluationException {
        return runContext.render(this.subscriptionId).as(String.class).orElseThrow(() -> new IllegalArgumentException("Missing required property `subscriptionId`"));
    }

    protected String rResourceGroupName(RunContext runContext) throws IllegalVariableEvaluationException {
        return runContext.render(this.resourceGroupName).as(String.class).orElseThrow(() -> new IllegalArgumentException("Missing required property `resourceGroupName`"));
    }

    protected String rWorkspaceName(RunContext runContext) throws IllegalVariableEvaluationException {
        return runContext.render(this.workspaceName).as(String.class).orElseThrow(() -> new IllegalArgumentException("Missing required property `workspaceName`"));
    }

    protected String studioUrl(RunContext runContext, String jobName) throws IllegalVariableEvaluationException {
        return "https://ml.azure.com/runs/%s?wsid=/subscriptions/%s/resourcegroups/%s/workspaces/%s".formatted(
            jobName,
            rSubscriptionId(runContext),
            rResourceGroupName(runContext),
            rWorkspaceName(runContext)
        );
    }
}
