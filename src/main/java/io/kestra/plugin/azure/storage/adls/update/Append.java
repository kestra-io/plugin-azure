package io.kestra.plugin.azure.storage.adls.update;

import com.azure.core.util.BinaryData;
import com.azure.storage.file.datalake.DataLakeFileClient;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.storage.adls.abstracts.AbstractDataLakeWithFile;
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
                id: azure_storage_datalake_append
                namespace: company.team

                tasks:
                  - id: read_file
                    type: io.kestra.plugin.azure.storage.adls.update.Append
                    endpoint: "https://yourblob.blob.core.windows.net"
                    sasToken: "{{ secret('SAS_TOKEN') }}"
                    fileSystem: "mydata"
                    filePath: "path/to/myfile"
                    data: "Text to append"
                """
        )
    }
)
@Schema(
    title = "Append data to an existing file in Azure Data Lake Storage."
)
public class Append extends AbstractDataLakeWithFile implements RunnableTask<VoidOutput> {
    @Schema(title = "Data")
    @NotNull
    protected Property<String> data;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        DataLakeFileClient client = this.dataLakeFileClient(runContext);

        final BinaryData binaryData = BinaryData.fromString(runContext.render(data).as(String.class).orElseThrow());
        final long fileSize = client.getProperties().getFileSize();

        client.append(binaryData, fileSize);
        client.flush(fileSize + binaryData.getLength(), true);

        runContext.metric(Counter.of("file.size", fileSize));
        runContext.metric(Counter.of("data.size", binaryData.getLength()));

        return null;
    }
}
