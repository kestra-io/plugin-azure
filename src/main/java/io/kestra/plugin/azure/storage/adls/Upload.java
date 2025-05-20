package io.kestra.plugin.azure.storage.adls;

import com.azure.core.util.BinaryData;
import com.azure.core.util.FluxUtil;
import com.azure.storage.common.implementation.Constants;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.microsoft.azure.storage.blob.BlobInputStream;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.storage.adls.abstracts.AbstractDataLakeWithFile;
import io.kestra.plugin.azure.storage.adls.models.AdlsFile;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import reactor.core.scheduler.Schedulers;

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
            title = "Download a json file and upload it to Azure Data Lake Storage.",
            code = """
                id: azure_data_lake_storage_upload
                namespace: company.team

                tasks:
                  - id: download_request
                    type: io.kestra.plugin.core.http.Download
                    uri: adls/product_data/product.json

                  - id: upload_file
                    type: io.kestra.plugin.azure.storage.adls.Upload
                    filePath: "path/to/file/product.json"
                    from: "{{ outputs.download_request.uri }}"
                    connectionString: "{{ secret('AZURE_CONNECTION_STRING') }}"
                    fileSystem: "tasks"
                    endpoint: "https://yourblob.blob.core.windows.net"
                """
        )
    }
)
@Schema(
    title = "Upload a file to the Azure Data Lake Storage."
)
public class Upload extends AbstractDataLakeWithFile implements RunnableTask<Upload.Output> {
    @Schema(
        title = "The file from the internal storage to upload to the Azure Data Lake Storage."
    )
    @PluginProperty(internalStorageURI = true)
    @NotNull
    private Property<String> from;


    @Override
    public Upload.Output run(RunContext runContext) throws Exception {
        URI fromUri = new URI(runContext.render(this.from).as(String.class).orElseThrow());

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
