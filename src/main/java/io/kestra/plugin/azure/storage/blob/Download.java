package io.kestra.plugin.azure.storage.blob;

import java.net.URI;

import com.azure.storage.blob.models.BlobProperties;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.shared.storage.blob.abstracts.AbstractBlobStorageWithSasObject;
import io.kestra.plugin.azure.shared.storage.blob.models.Blob;
import io.kestra.plugin.azure.storage.blob.services.BlobService;
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
                id: azure_storage_blob_download
                namespace: company.team

                tasks:
                  - id: download
                    type: io.kestra.plugin.azure.storage.blob.Download
                    endpoint: "https://yourblob.blob.core.windows.net"
                    connectionString: "DefaultEndpointsProtocol=...=="
                    container: "mydata"
                    name: "myblob"
                """
        ),
        @Example(
            title = "Download with MD5 verification against the server's Content-MD5",
            full = true,
            code = """
                id: azure_storage_blob_download_verified
                namespace: company.team

                tasks:
                  - id: download
                    type: io.kestra.plugin.azure.storage.blob.Download
                    endpoint: "https://yourblob.blob.core.windows.net"
                    connectionString: "DefaultEndpointsProtocol=...=="
                    container: "mydata"
                    name: "myblob"
                    validateChecksum: true
                    failOnMissingChecksum: true
                """
        )
    }
)
@Schema(
    title = "Download a blob to Kestra storage",
    description = "Fetches a blob and stores it in internal storage, returning metadata and the downloaded URI."
)
public class Download extends AbstractBlobStorageWithSasObject implements RunnableTask<Download.Output>, SingleFileChecksumValidatedInterface, io.kestra.core.models.WorkerJobLifecycle {

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
        com.azure.storage.blob.BlobServiceAsyncClient asyncClient = BlobService.asyncClient(
            this.endpoint,
            this.connectionString,
            this.sharedKeyAccountName,
            this.sharedKeyAccountAccessKey,
            this.sasToken,
            runContext
        );
        com.azure.storage.blob.BlobContainerAsyncClient containerClient = asyncClient.getBlobContainerAsyncClient(runContext.render(this.container).as(String.class).orElseThrow());
        com.azure.storage.blob.BlobAsyncClient blobAsyncClient = containerClient.getBlobAsyncClient(runContext.render(this.name).as(String.class).orElseThrow());

        ChecksumValidator.Options checksumOptions = ChecksumValidator.resolve(
            runContext, validateChecksum, failOnMissingChecksum, expectedChecksum, checksumAlgorithm
        );

        java.io.File tempFile = runContext.workingDir().createTempFile(io.kestra.core.utils.FileUtils.getExtension(blobAsyncClient.getBlobName())).toFile();

        java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(1);
        this.latchRef.set(latch);
        java.util.concurrent.atomic.AtomicReference<Throwable> error = new java.util.concurrent.atomic.AtomicReference<>();
        java.util.concurrent.atomic.AtomicReference<BlobProperties> propsRef = new java.util.concurrent.atomic.AtomicReference<>();

        reactor.core.Disposable d = blobAsyncClient.downloadToFile(tempFile.getAbsolutePath(), true)
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

        BlobProperties blobProperties = propsRef.get();
        runContext.metric(io.kestra.core.models.executions.metrics.Counter.of("file.size", blobProperties.getBlobSize()));

        ChecksumValidator.verify(
            runContext,
            tempFile,
            blobProperties.getContentMd5(),
            checksumOptions,
            blobAsyncClient.getBlobName()
        );

        URI uri = runContext.storage().putFile(tempFile);

        return Output
            .builder()
            .blob(
                Blob.builder()
                    .name(blobAsyncClient.getBlobName())
                    .container(containerClient.getBlobContainerName())
                    .size(blobProperties.getBlobSize())
                    .lastModified(blobProperties.getLastModified())
                    .eTag(blobProperties.getETag())
                    .uri(uri)
                    .build()
            )
            .build();
    }

    @SuperBuilder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Downloaded blob",
            description = "Metadata plus internal storage URI of the fetched blob"
        )
        private final Blob blob;
    }
}
