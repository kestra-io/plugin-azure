package io.kestra.plugin.azure.storage.adls;

import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.storage.adls.abstracts.AbstractDataLakeConnection;
import io.kestra.plugin.azure.storage.adls.abstracts.AbstractDataLakeStorageInterface;
import io.kestra.plugin.azure.storage.adls.models.AdlsFile;
import io.kestra.plugin.azure.storage.adls.services.DataLakeService;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.net.URI;
import java.util.AbstractMap;
import java.util.Map;
import java.util.stream.Collectors;

import static io.kestra.core.utils.Rethrow.throwFunction;

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
                id: azure_storage_datalake_readq
                namespace: company.team

                tasks:
                  - id: read_file
                    type: io.kestra.plugin.azure.storage.adls.Reads
                    connectionString: "{{ secret('AZURE_CONNECTION_STRING') }}"
                    fileSystem: "tasks"
                    endpoint: "https://yourblob.blob.core.windows.net"
                    directoryPath: "path/to/my/directory/"
                """
        )
    }
)
@Schema(
    title = "Read all files from an Azure Data Lake Storage directory."
)
public class Reads extends AbstractDataLakeConnection implements RunnableTask<Reads.Output>, AbstractDataLakeStorageInterface {
    @Schema(title = "Directory Name")
    @NotNull
    protected Property<String> directoryPath;

    protected Property<String> fileSystem;

    @Schema(
        title = "The maximum number of files to read",
        description = "Limits the number of files read. If not specified, all matching files will be read."
    )
    @Builder.Default
    private Property<Integer> maxFiles = Property.ofValue(25);

    @Override
    public Reads.Output run(RunContext runContext) throws Exception {
        List task = List.builder()
            .id(this.id)
            .type(io.kestra.plugin.azure.storage.adls.List.class.getName())
            .endpoint(this.endpoint)
            .fileSystem(this.fileSystem)
            .directoryPath(this.directoryPath)
            .sasToken(this.sasToken)
            .connectionString(this.connectionString)
            .sharedKeyAccountName(this.sharedKeyAccountName)
            .sharedKeyAccountAccessKey(this.sharedKeyAccountAccessKey)
            .maxFiles(this.maxFiles)
            .build();
        List.Output run = task.run(runContext);

        DataLakeServiceClient client = this.dataLakeServiceClient(runContext);
        DataLakeFileSystemClient fileSystemClient = client.getFileSystemClient(runContext.render(this.fileSystem).as(String.class).orElseThrow());

        java.util.List<AdlsFile> list = run
            .getFiles()
            .stream()
            .map(throwFunction(object -> {
                DataLakeFileClient fileClient = fileSystemClient.getFileClient(object.getName());
                URI readFileUri = DataLakeService.read(runContext, fileClient);

                return AdlsFile.of(fileClient)
                    .withUri(readFileUri);
            }))
            .toList();

        Map<String, URI> outputFiles = list.stream()
            .filter(file -> !file.getName().endsWith("/"))
            .map(file -> new AbstractMap.SimpleEntry<>(file.getName(), file.getUri()))
            .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));

        return Output
            .builder()
            .files(list)
            .outputFiles(outputFiles)
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The list of files."
        )
        private final java.util.List<AdlsFile> files;

        @Schema(
            title = "The downloaded files as a map of from/to URIs."
        )
        private final Map<String, URI> outputFiles;
    }
}
