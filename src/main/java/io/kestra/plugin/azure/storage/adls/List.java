package io.kestra.plugin.azure.storage.adls;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
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
    title = "List files and directories in Azure Data Lake Storage",
    description = "Lists the files and directories under a given Data Lake Storage directory path."
)
public class List extends AbstractDataLakeConnection implements RunnableTask<List.Output>, AbstractDataLakeStorageInterface, io.kestra.core.models.WorkerJobLifecycle {
    @Schema(title = "Directory path", description = "Full path to the directory")
    @NotNull
    @PluginProperty(group = "main")
    protected Property<String> directoryPath;

    @PluginProperty(group = "main")
    protected Property<String> fileSystem;

    @Schema(
        title = "The maximum number of files to return",
        description = "Limits the number of files returned by the list operation. If not specified, all matching files will be returned."
    )
    @Builder.Default
    @PluginProperty(group = "processing")
    private Property<Integer> maxFiles = Property.ofValue(25);

    @lombok.Builder.Default
    @lombok.Getter(lombok.AccessLevel.NONE)
    private transient java.util.concurrent.atomic.AtomicReference<reactor.core.Disposable> disposable = new java.util.concurrent.atomic.AtomicReference<>();

    @lombok.Builder.Default
    @lombok.Getter(lombok.AccessLevel.NONE)
    private transient java.util.concurrent.atomic.AtomicBoolean killed = new java.util.concurrent.atomic.AtomicBoolean(false);

    @lombok.Builder.Default
    @lombok.Getter(lombok.AccessLevel.NONE)
    private transient java.util.concurrent.atomic.AtomicReference<java.util.concurrent.CountDownLatch> latchRef = new java.util.concurrent.atomic.AtomicReference<>();

    public void kill() {
        killed.set(true);
        reactor.core.Disposable current = disposable.getAndSet(null);
        if (current != null) {
            current.dispose();
        }
        java.util.concurrent.CountDownLatch latch = latchRef.get();
        if (latch != null) {
            latch.countDown();
        }
    }

    @Override
    public List.Output run(RunContext runContext) throws Exception {
        com.azure.storage.file.datalake.DataLakeServiceAsyncClient dataLakeServiceAsyncClient = DataLakeService.asyncClient(
            runContext.render(this.endpoint).as(String.class).orElse(null),
            runContext.render(this.connectionString).as(String.class).orElse(null),
            runContext.render(this.sharedKeyAccountName).as(String.class).orElse(null),
            runContext.render(this.sharedKeyAccountAccessKey).as(String.class).orElse(null),
            runContext.render(this.sasToken).as(String.class).orElse(null),
            runContext
        );
        com.azure.storage.file.datalake.DataLakeFileSystemAsyncClient fileSystemAsyncClient = dataLakeServiceAsyncClient
            .getFileSystemAsyncClient(runContext.render(fileSystem).as(String.class).orElseThrow());

        String rDirectoryPath = runContext.render(directoryPath).as(String.class).orElseThrow();
        com.azure.storage.file.datalake.models.ListPathsOptions options = new com.azure.storage.file.datalake.models.ListPathsOptions();
        options.setPath(rDirectoryPath);
        Integer rMaxFiles = runContext.render(this.maxFiles).as(Integer.class).orElse(25);

        java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(1);
        this.latchRef.set(latch);
        java.util.concurrent.atomic.AtomicReference<Throwable> error = new java.util.concurrent.atomic.AtomicReference<>();
        java.util.List<AdlsFile> fileList = new java.util.ArrayList<>();

        reactor.core.Disposable d = fileSystemAsyncClient.listPaths(options)
            .map(
                item -> AdlsFile.builder()
                    .fileSystem(fileSystemAsyncClient.getFileSystemName())
                    .name(item.getName())
                    .fileName(item.getName().substring(item.getName().lastIndexOf('/') + 1))
                    .size(item.getContentLength())
                    .eTag(item.getETag())
                    .lastModifed(item.getLastModified() != null ? item.getLastModified().toInstant() : null)
                    .creationTime(item.getCreationTime() != null ? item.getCreationTime().toInstant() : null)
                    .isDirectory(Boolean.TRUE.equals(item.isDirectory()))
                    .owner(item.getOwner())
                    .group(item.getGroup())
                    .permissions(item.getPermissions())
                    .build()
            )
            .take(rMaxFiles)
            .subscribe(
                fileList::add,
                err ->
                {
                    error.set(err);
                    latch.countDown();
                },
                latch::countDown
            );
        this.disposable.set(d);
        if (killed.get()) {
            this.kill();
        }

        latch.await();
        this.disposable.set(null);
        this.latchRef.set(null);

        if (killed.get()) {
            throw new InterruptedException("Task was killed");
        }

        if (error.get() != null) {
            if (error.get() instanceof Exception e)
                throw e;
            throw new Exception(error.get());
        }

        return Output.builder()
            .files(fileList)
            .build();
    }

    @SuperBuilder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The list of files"
        )
        private final java.util.List<AdlsFile> files;
    }
}
