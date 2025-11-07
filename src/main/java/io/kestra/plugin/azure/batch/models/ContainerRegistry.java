
package io.kestra.plugin.azure.batch.models;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Value;

@Builder
@Value
public class ContainerRegistry {
    @Schema(
        title = "The registry server URL.",
        description = "If omitted, the default is \"docker.io\"."
    )
    Property<String> registryServer;

    @Schema(
        title = "The user name to log into the registry server."
    )
    Property<String> userName;

    @Schema(
        title = "The password to log into the registry server."
    )
    Property<String> password;

    @Schema(
        title = "The reference to the user assigned identity to use to access the Azure Container Registry instead of username and password."
    )
    @PluginProperty(dynamic = true)
    ComputeNodeIdentityReference identityReference;

    public com.microsoft.azure.batch.protocol.models.ContainerRegistry to(RunContext runContext) throws IllegalVariableEvaluationException {
        return new com.microsoft.azure.batch.protocol.models.ContainerRegistry()
            .withRegistryServer(runContext.render(this.registryServer).as(String.class).orElse(null))
            .withUserName(runContext.render(this.userName).as(String.class).orElse(null))
            .withPassword(runContext.render(this.password).as(String.class).orElse(null))
            .withIdentityReference(this.identityReference == null ? null : this.identityReference.to(runContext));
    }
}
