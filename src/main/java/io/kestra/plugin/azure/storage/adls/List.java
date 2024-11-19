package io.kestra.plugin.azure.storage.adls;

import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.storage.adls.abstracts.AbstractDataLakeConnection;
import io.kestra.plugin.azure.storage.adls.abstracts.AbstractDataLakeStorageInterface;
import io.kestra.plugin.azure.storage.adls.models.AdlsFile;
import io.kestra.plugin.azure.storage.adls.services.DataLakeService;
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
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: azure_storage_datalake_list
                namespace: company.team

                tasks:
                  - id: read_file
                    type: io.kestra.plugin.azure.storage.adls.List
                    endpoint: "https://yourblob.blob.core.windows.net"
                    sasToken: "{{ secret('SAS_TOKEN') }}"
                    fileSystem: "mydata"
                    directoryName: "myDirectory"
                """
        )
    }
)
@Schema(
    title = "Upload a file to the Azure Data Lake Storage."
)
public class List extends AbstractDataLakeConnection implements RunnableTask<List.Output>, AbstractDataLakeStorageInterface {
    @Schema(title = "Directory Name")
    @PluginProperty(dynamic = true)
    @NotNull
    protected String directoryName;

    protected String fileSystem;

    @Override
    public List.Output run(RunContext runContext) throws Exception {
        DataLakeServiceClient dataLakeServiceClient = this.dataLakeServiceClient(runContext);
        DataLakeFileSystemClient fileSystemClient = dataLakeServiceClient.getFileSystemClient(runContext.render(fileSystem));

        java.util.List<AdlsFile> fileList = DataLakeService.list(runContext, fileSystemClient, directoryName);

        return Output.builder()
            .files(fileList)
            .build();
    }

    @SuperBuilder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The list of file."
        )
        private final java.util.List<AdlsFile> files;
    }
}
