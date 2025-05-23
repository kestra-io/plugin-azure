package io.kestra.plugin.azure.storage.adls;

import com.azure.storage.file.datalake.DataLakeFileClient;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.storage.adls.abstracts.AbstractDataLakeWithFile;
import io.kestra.plugin.azure.storage.adls.models.AdlsFile;
import io.kestra.plugin.azure.storage.adls.services.DataLakeService;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.net.URI;

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
                id: azure_storage_datalake_read
                namespace: company.team

                tasks:
                  - id: read_file
                    type: io.kestra.plugin.azure.storage.adls.Read
                    connectionString: "{{ secret('AZURE_CONNECTION_STRING') }}"
                    fileSystem: "tasks"
                    endpoint: "https://yourblob.blob.core.windows.net"
                    filePath: "full/path/to/file.txt"

                  - id: log_size
                    type: io.kestra.plugin.core.debug.Echo
                    level: INFO
                    format: " {{ outputs.read_file.file.size }}"
                """
        )
    }
)
@Schema(
    title = "Read a file from Azure Data Lake Storage."
)
public class Read extends AbstractDataLakeWithFile implements RunnableTask<Read.Output> {
    @Override
    public Output run(RunContext runContext) throws Exception {
        DataLakeFileClient client = this.dataLakeFileClient(runContext);
        URI readFileUri = DataLakeService.read(runContext, client);

        return Output
            .builder()
            .file(AdlsFile.of(client).withUri(readFileUri))
            .build();
    }

    @SuperBuilder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The downloaded file."
        )
        private final AdlsFile file;
    }
}
