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
        title = "Registry server",
        description = "Container registry hostname; defaults to docker.io"
    )
    Property<String> registryServer;

    @Schema(title = "Registry username")
    Property<String> userName;

    @Schema(title = "Registry password")
    Property<String> password;

    @Schema(
        title = "User-assigned identity for ACR",
        description = "Use managed identity instead of username/password when supported by the registry"
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
