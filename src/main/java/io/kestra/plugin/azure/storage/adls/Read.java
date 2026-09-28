package io.kestra.plugin.azure.storage.adls;

import java.net.URI;
import java.util.Base64;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.storage.adls.abstracts.AbstractDataLakeWithFile;
import io.kestra.plugin.azure.storage.adls.models.AdlsFile;
import io.kestra.plugin.azure.storage.adls.services.DataLakeService;
import io.kestra.plugin.azure.storage.services.ChecksumValidator;
import io.kestra.plugin.azure.storage.services.SingleFileChecksumValidatedInterface;

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
    title = "Read a file from Azure Data Lake Storage",
    description = "Read a file from Azure Data Lake Storage using the Azure SDK."
)
public class Read extends AbstractDataLakeWithFile implements RunnableTask<Read.Output>, SingleFileChecksumValidatedInterface, io.kestra.core.models.WorkerJobLifecycle {

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

    private Property<Boolean> validateChecksum;

    private Property<Boolean> failOnMissingChecksum;

    private Property<String> expectedChecksum;

    private Property<ChecksumValidator.Algorithm> checksumAlgorithm;

    @Override
    public Output run(RunContext runContext) throws Exception {
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
        com.azure.storage.file.datalake.DataLakeFileAsyncClient client = fileSystemAsyncClient.getFileAsyncClient(runContext.render(filePath).as(String.class).orElseThrow());

        ChecksumValidator.Options checksumOptions = ChecksumValidator.resolve(
            runContext, validateChecksum, failOnMissingChecksum, expectedChecksum, checksumAlgorithm
        );

        java.io.File tempFile = runContext.workingDir().createTempFile(io.kestra.core.utils.FileUtils.getExtension(client.getFileName())).toFile();

        java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(1);
        this.latchRef.set(latch);
        java.util.concurrent.atomic.AtomicReference<Throwable> error = new java.util.concurrent.atomic.AtomicReference<>();
        java.util.concurrent.atomic.AtomicReference<com.azure.storage.file.datalake.models.PathProperties> propsRef = new java.util.concurrent.atomic.AtomicReference<>();

        reactor.core.Disposable d = client.readToFile(tempFile.getAbsolutePath(), true)
            .subscribe(
                props ->
                {
                    propsRef.set(props);
                    latch.countDown();
                },
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

        com.azure.storage.file.datalake.models.PathProperties pathProperties = propsRef.get();
        runContext.metric(io.kestra.core.models.executions.metrics.Counter.of("file.size", pathProperties.getFileSize()));

        ChecksumValidator.verify(
            runContext,
            tempFile,
            pathProperties.getContentMd5(),
            checksumOptions,
            client.getFilePath()
        );

        URI readFileUri = runContext.storage().putFile(tempFile);

        return Output
            .builder()
            .file(
                AdlsFile.builder()
                    .name(client.getFilePath())
                    .lastModifed(pathProperties.getLastModified().toInstant())
                    .eTag(pathProperties.getETag())
                    .creationTime(pathProperties.getCreationTime().toInstant())
                    .size(pathProperties.getFileSize())
                    .isDirectory(pathProperties.isDirectory())
                    .contentMd5(pathProperties.getContentMd5() != null ? Base64.getEncoder().encodeToString(pathProperties.getContentMd5()) : null)
                    .uri(readFileUri)
                    .build()
            )
            .build();
    }

    @SuperBuilder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The downloaded file"
        )
        private final AdlsFile file;
    }
}
