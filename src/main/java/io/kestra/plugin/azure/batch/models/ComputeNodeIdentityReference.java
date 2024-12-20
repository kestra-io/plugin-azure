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
public class ComputeNodeIdentityReference {
    @Schema(
        title = "The ARM resource ID of the user assigned identity."
    )
    Property<String> resourceId;

    public com.microsoft.azure.batch.protocol.models.ComputeNodeIdentityReference to(RunContext runContext) throws IllegalVariableEvaluationException {
        return new com.microsoft.azure.batch.protocol.models.ComputeNodeIdentityReference()
            .withResourceId(runContext.render(this.resourceId).as(String.class).orElse(null));
    }
}
