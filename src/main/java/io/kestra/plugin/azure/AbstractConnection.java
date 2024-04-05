package io.kestra.plugin.azure;

import io.kestra.core.models.tasks.Task;
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
    protected String endpoint;

    public boolean valid() {
        return this.endpoint != null;
    }
}
