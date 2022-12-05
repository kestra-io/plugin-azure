package io.kestra.plugin.azure.batch.models;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Value;

@Builder
@Value
public class ContainerRegistry {
    @Schema(
        title = "The registry URL.",
        description = "If omitted, the default is \"docker.io\"."
    )
    @PluginProperty(dynamic = true)
    String registryServer;

    @Schema(
        title = "The user name to log into the registry server."
    )
    @PluginProperty(dynamic = true)
    String userName;

    @Schema(
        title = "The password to log into the registry server."
    )
    @PluginProperty(dynamic = true)
    String password;

    @Schema(
        title = "The reference to the user assigned identity to use to access an Azure Container Registry instead of username and password."
    )
    @PluginProperty(dynamic = true)
    ComputeNodeIdentityReference identityReference;

    public com.microsoft.azure.batch.protocol.models.ContainerRegistry to(RunContext runContext) throws IllegalVariableEvaluationException {
        return new com.microsoft.azure.batch.protocol.models.ContainerRegistry()
            .withRegistryServer(runContext.render(this.registryServer))
            .withUserName(runContext.render(this.userName))
            .withPassword(runContext.render(this.password))
            .withIdentityReference(this.identityReference == null ? null : this.identityReference.to(runContext));
    }
}
