package io.kestra.plugin.azure.batch;

import io.kestra.plugin.azure.AbstractConnection;
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
public abstract class AbstractBatch extends AbstractConnection {
    protected String account;
    protected String accessKey;
}
