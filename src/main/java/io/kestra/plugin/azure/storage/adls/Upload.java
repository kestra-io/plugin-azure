package io.kestra.plugin.azure.storage.adls;

import com.azure.core.util.BinaryData;
import com.azure.storage.file.datalake.DataLakeFileClient;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
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

import java.io.InputStream;
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
                id: azure_storage_datalake_upload
                namespace: company.team

                tasks:
                  - id: read_file
                    type: io.kestra.plugin.azure.storage.adls.Upload
                    endpoint: "https://yourblob.blob.core.windows.net"
                    sasToken: "{{ secret('SAS_TOKEN') }}"
                    fileSystem: "mydata"
                    fileName: "path/to/myfile"
                    from: "{{ inputs.myfile }}"
                """
        )
    }
)
@Schema(
    title = "Upload a file to the Azure Data Lake Storage."
)
public class Upload extends AbstractDataLakeWithFileName implements RunnableTask<Upload.Output> {
    @Schema(
        title = "The file from the internal storage to upload to the Azure Data Lake Storage."
    )
    @PluginProperty(dynamic = true)
    private String from;


    @Override
    public Upload.Output run(RunContext runContext) throws Exception {
        URI fromUri = new URI(runContext.render(this.from));

        try (InputStream is = runContext.storage().getFile(fromUri)) {
            DataLakeFileClient fileClient = this.dataLakeFileClient(runContext);
            fileClient.upload(BinaryData.fromStream(is), true);

            runContext.metric(Counter.of("file.size", fileClient.getProperties().getFileSize()));

            return Output
                .builder()
                .file(AdlsFile.of(fileClient))
                .build();
        }

    }

    @SuperBuilder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The uploaded file."
        )
        private final AdlsFile file;
    }
}
