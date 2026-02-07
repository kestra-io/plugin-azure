package io.kestra.plugin.azure.storage.adls;

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

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "List all files and directories in a specific Azure Data Lake Storage directory and log each file data output.",
            code = """
                id: azure_data_lake_storage_list
                namespace: company.team

                tasks:
                  - id: list_files_in_dir
                    type: io.kestra.plugin.azure.storage.adls.List
                    connectionString: "{{ secret('AZURE_CONNECTION_STRING') }}"
                    fileSystem: "tasks"
                    endpoint: "https://yourblob.blob.core.windows.net"
                    directoryPath: "path/to/my/directory/"

                  - id: for_each_file
                      type: io.kestra.plugin.core.flow.EachParallel
                      value: "{{ outputs.list_files_in_dir.files }}"
                      tasks:
                        - id: log_file_name
                          type: io.kestra.plugin.core.debug.Echo
                          level: DEBUG
                          format: "{{ taskrun.value }}"
                """
        )
    }
)
@Schema(
    title = "Upload a file to Azure Data Lake Storage."
)
public class List extends AbstractDataLakeConnection implements RunnableTask<List.Output>, AbstractDataLakeStorageInterface {
    @Schema(title = "Directory path", description = "Full path to the directory")
    @NotNull
    protected Property<String> directoryPath;

    protected Property<String> fileSystem;

    @Schema(
        title = "The maximum number of files to return",
        description = "Limits the number of files returned by the list operation. If not specified, all matching files will be returned."
    )
    @Builder.Default
    private Property<Integer> maxFiles = Property.ofValue(25);

    @Override
    public List.Output run(RunContext runContext) throws Exception {
        DataLakeServiceClient dataLakeServiceClient = this.dataLakeServiceClient(runContext);
        DataLakeFileSystemClient fileSystemClient = dataLakeServiceClient.getFileSystemClient(runContext.render(fileSystem).as(String.class).orElseThrow());

        java.util.List<AdlsFile> fileList = DataLakeService.list(fileSystemClient, runContext.render(directoryPath).as(String.class).orElseThrow());

        Integer rMaxFiles = runContext.render(this.maxFiles).as(Integer.class).orElse(25);

        if (fileList.size() > rMaxFiles) {
            runContext.logger().warn(
                "Listing returned {} files but maxFiles limit is {}. "
                    + "Only the first {} files will be returned. "
                    + "Increase the maxFiles property if you need more files.",
                fileList.size(),
                rMaxFiles,
                rMaxFiles
            );
            fileList = fileList.subList(0, rMaxFiles);
        }

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
