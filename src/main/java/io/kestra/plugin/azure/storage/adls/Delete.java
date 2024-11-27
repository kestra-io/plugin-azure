package io.kestra.plugin.azure.storage.adls;

import com.azure.storage.file.datalake.DataLakeFileClient;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.storage.adls.abstracts.AbstractDataLakeWithFileName;
import io.kestra.plugin.azure.storage.adls.models.AdlsFile;
import io.swagger.v3.oas.annotations.media.Schema;
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
            title = "Delete an existing file in Azure Data Lake Storage.",
            code = """
                id: azure_storage_datalake_delete
                namespace: company.team
                      
                tasks:
                  - id: delete_file
                    type: io.kestra.plugin.azure.storage.adls.Delete
                    filePath: "full/path/to/file.txt"
                    fileSystem: "tasks"
                    endpoint: "https://yourblob.blob.core.windows.net"
                    connectionString: "{{ secret('AZURE_CONNECTION_STRING') }}"
                """
        )
    }
)
@Schema(
    title = "Delete a file from Azure Data Lake Storage."
)
public class Delete extends AbstractDataLakeWithFileName implements RunnableTask<Delete.Output> {

    @Override
    public Delete.Output run(RunContext runContext) throws Exception {

        DataLakeFileClient fileClient = this.dataLakeFileClient(runContext);
        Output output = null;
        if (Boolean.TRUE.equals(fileClient.exists())) {
            output = Output
                .builder()
                .file(AdlsFile.of(fileClient))
                .build();
        }
        fileClient.delete();

        return output;

    }

    @SuperBuilder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The deleted file."
        )
        private final AdlsFile file;
    }
}
