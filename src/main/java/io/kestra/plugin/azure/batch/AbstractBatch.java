package io.kestra.plugin.azure.batch;

import com.microsoft.azure.batch.BatchClient;
import com.microsoft.azure.batch.auth.BatchSharedKeyCredentials;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.AbstractConnection;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractBatch extends AbstractConnection {
    @Schema(
        title = "The Batch account name."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String account;

    @Schema(
        title = "The Batch access key."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String accessKey;

    protected BatchClient client(RunContext runContext) throws IllegalVariableEvaluationException {
        return BatchClient.open(new BatchSharedKeyCredentials(
            runContext.render(this.endpoint),
            runContext.render(this.account),
            runContext.render(this.accessKey)
        ));
    }
}
