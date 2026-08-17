package io.kestra.plugin.azure.logicapps;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;

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
public abstract class AbstractLogicAppsWorkflowTask extends AbstractLogicAppsTask {
    @Schema(title = "Workflow name", description = "Name of the Azure Logic App workflow.")
    @NotNull
    @PluginProperty(group = "main")
    protected Property<String> workflowName;
}
