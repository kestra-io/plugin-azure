package io.kestra.plugin.azure.storage.adls.abstracts;

import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
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
public abstract class AbstractDataLakeWithFile extends AbstractDataLakeConnection implements AbstractDataLakeStorageInterface {

    @Schema(title = "File path", description = "Full path of the file in its file system")
    @PluginProperty(dynamic = true)
    @NotNull
    protected String filePath;

    protected String fileSystem;

    protected DataLakeFileClient dataLakeFileClient(RunContext runContext) throws IllegalVariableEvaluationException {
        DataLakeFileSystemClient dataLakeFileSystemClient = this.dataLakeServiceClient(runContext)
            .getFileSystemClient(runContext.render(this.fileSystem));
        return dataLakeFileSystemClient.getFileClient(runContext.render(filePath));
    }
}
