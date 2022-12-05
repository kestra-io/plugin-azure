package io.kestra.plugin.azure.batch.models;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Value;

import javax.validation.constraints.NotNull;

@Builder
@Value
public class OutputFile {
    @Schema(
        title = "A pattern indicating which file(s) to upload.",
        description = "Both relative and absolute paths are supported. Relative paths are relative to the Task working " +
            "directory. The following wildcards are supported: * matches 0 or more characters (for example pattern " +
            "abc* would match abc or abcdef), ** matches any directory, ? matches any single character, " +
            "[abc] matches one character in the brackets, and [a-c] matches one character in the range. " +
            "Brackets can include a negation to match any character not specified (for example [!abc] matches any " +
            "character but a, b, or c). If a file name starts with \".\" it is ignored by default but may be " +
            "matched by specifying it explicitly (for example *.gif will not match .a.gif, but .*.gif will). " +
            "A simple example: **\\*.txt matches any file that does not start in '.' and ends with .txt in the " +
            "Task working directory or any subdirectory. If the filename contains a wildcard character it can be " +
            "escaped using brackets (for example abc[*] would match a file named abc*). Note that both \\ and / " +
            "are treated as directory separators on Windows, but only / is on Linux. " +
            "Environment variables (%var% on Windows or $var on Linux) are expanded prior to the pattern being applied."
    )
    @PluginProperty(dynamic = true)
    String filePattern;

    @Schema(
        title = "The destination for the output file(s)."
    )
    @PluginProperty(dynamic = false)
    @NotNull
    OutputFileDestination destination;

    @Schema(
        title = "Additional options for the upload operation, including under what conditions to perform the upload."
    )
    @PluginProperty(dynamic = false)
    @NotNull
    @Builder.Default
    OutputFileUploadOptions uploadOptions = OutputFileUploadOptions.builder().build();

    public com.microsoft.azure.batch.protocol.models.OutputFile to(RunContext runContext) throws IllegalVariableEvaluationException {
        return new com.microsoft.azure.batch.protocol.models.OutputFile()
            .withFilePattern(runContext.render(this.filePattern))
            .withDestination(this.destination == null ? null : this.destination.to(runContext))
            .withUploadOptions(this.uploadOptions == null ? null : this.uploadOptions.to(runContext));
    }
}
