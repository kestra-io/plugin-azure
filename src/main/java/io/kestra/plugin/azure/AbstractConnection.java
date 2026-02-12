package io.kestra.plugin.azure;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
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
public abstract class AbstractConnection extends Task implements AbstractConnectionInterface {
    @NotNull
    @Schema(
        title = "Service endpoint URL",
        description = "Base HTTPS endpoint of the target service."
    )
    protected Property<String> endpoint;
}
