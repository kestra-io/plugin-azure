package io.kestra.plugin.azure.batch.models;

import com.microsoft.azure.batch.protocol.models.OutputFileUploadCondition;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

import javax.validation.constraints.NotNull;

@Builder
@Value
@Jacksonized
public class OutputFileUploadOptions {
    @Schema(
        title = "The conditions under which the Task output file or set of files should be uploaded."
    )
    @PluginProperty(dynamic = false)
    @NotNull
    @Builder.Default
    OutputFileUploadCondition uploadCondition = OutputFileUploadCondition.TASK_COMPLETION;

    public com.microsoft.azure.batch.protocol.models.OutputFileUploadOptions to(RunContext runContext) throws IllegalVariableEvaluationException {
        return new com.microsoft.azure.batch.protocol.models.OutputFileUploadOptions()
            .withUploadCondition(this.uploadCondition);
    }
}
