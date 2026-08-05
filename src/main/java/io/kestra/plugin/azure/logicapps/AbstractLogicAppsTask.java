package io.kestra.plugin.azure.logicapps;

import com.azure.core.management.AzureEnvironment;
import com.azure.core.management.profile.AzureProfile;
import com.azure.resourcemanager.logic.LogicManager;

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

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractLogicAppsTask extends AbstractAzureIdentityConnection {
    @Schema(title = "Subscription ID", description = "Azure subscription GUID that owns the Logic App workflow.")
    @NotNull
    @PluginProperty(group = "connection")
    protected Property<String> subscriptionId;

    @Schema(title = "Resource group name", description = "Azure resource group containing the Logic App workflow.")
    @NotNull
    @PluginProperty(group = "main")
    protected Property<String> resourceGroupName;

    protected LogicManager logicManager(RunContext runContext) throws IllegalVariableEvaluationException {
        String tenant = runContext.render(this.tenantId).as(String.class).orElse(null);
        String subscription = runContext.render(this.subscriptionId).as(String.class).orElseThrow();

        return LogicManager.authenticate(
            credentials(runContext),
            new AzureProfile(tenant, subscription, AzureEnvironment.AZURE)
        );
    }

    @FunctionalInterface
    protected interface ThrowingSupplier<T> {
        T get() throws Exception;
    }

    protected <T> T withAzureContext(RunContext runContext, String message, ThrowingSupplier<T> supplier) throws Exception {
        try {
            return supplier.get();
        } catch (Exception e) {
            throw new Exception(message, e);
        }
    }
}
