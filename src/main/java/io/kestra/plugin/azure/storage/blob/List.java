package io.kestra.plugin.azure.storage.blob;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.shared.storage.blob.abstracts.AbstractBlobStorageContainerInterface;
import io.kestra.plugin.azure.shared.storage.blob.abstracts.AbstractBlobStorageWithSas;
import io.kestra.plugin.azure.shared.storage.blob.abstracts.ListInterface;
import io.kestra.plugin.azure.shared.storage.blob.models.Blob;
import io.kestra.plugin.azure.storage.blob.services.BlobService;

import io.swagger.v3.oas.annotations.media.Schema;
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
            code = """
                id: azure_storage_blob_list
                namespace: company.team

                tasks:
                  - id: list
                    type: io.kestra.plugin.azure.storage.blob.List
                    endpoint: "https://yourblob.blob.core.windows.net"
                    connectionString: "DefaultEndpointsProtocol=...=="
                    container: "mydata"
                    prefix: "sub-dir"
                    delimiter: "/"
                """
        )
    },
    metrics = {
        @Metric(name = "blobs.count", type = Counter.TYPE, description = "The total number of blobs listed.")
    }
)
@Schema(
    title = "List blob objects in an Azure Blob Storage container",
    description = "List blob objects in an Azure Blob Storage container using the Azure SDK."
)
public class List extends AbstractBlobStorageWithSas implements RunnableTask<List.Output>, ListInterface, AbstractBlobStorageContainerInterface, io.kestra.core.models.WorkerJobLifecycle {

    @Builder.Default
    @lombok.Getter(lombok.AccessLevel.NONE)
    private transient java.util.concurrent.atomic.AtomicReference<reactor.core.Disposable> disposable = new java.util.concurrent.atomic.AtomicReference<>();

    @Builder.Default
    @lombok.Getter(lombok.AccessLevel.NONE)
    private transient java.util.concurrent.atomic.AtomicBoolean killed = new java.util.concurrent.atomic.AtomicBoolean(false);

    @Builder.Default
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

    @PluginProperty(group = "main")
    private Property<String> container;

    @PluginProperty(group = "source")
    private Property<String> prefix;

    @PluginProperty(group = "processing")
    protected Property<String> regexp;

    @PluginProperty(group = "processing")
    protected Property<String> delimiter;

    @Builder.Default
    @PluginProperty(group = "processing")
    private Property<Filter> filter = Property.ofValue(Filter.FILES);

    @Schema(
        title = "The maximum number of files to return",
        description = "Limits the number of blobs returned by the list operation. If not specified, all matching blobs will be returned."
    )
    @Builder.Default
    @PluginProperty(group = "processing")
    private Property<Integer> maxFiles = Property.ofValue(25);

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
        com.azure.storage.blob.BlobContainerAsyncClient containerClient = asyncClient.getBlobContainerAsyncClient(runContext.render(this.container).as(String.class).orElse(null));

        com.azure.storage.blob.models.ListBlobsOptions options = new com.azure.storage.blob.models.ListBlobsOptions()
            .setPrefix(runContext.render(this.prefix).as(String.class).orElse(null));

        String renderedRegexp = runContext.render(this.regexp).as(String.class).orElse(null);
        Filter renderedFilter = runContext.render(this.filter).as(Filter.class).orElse(Filter.FILES);
        Integer rMaxFiles = runContext.render(this.maxFiles).as(Integer.class).orElse(25);

        java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(1);
        this.latchRef.set(latch);
        java.util.concurrent.atomic.AtomicReference<Throwable> error = new java.util.concurrent.atomic.AtomicReference<>();
        java.util.List<Blob> list = new java.util.ArrayList<>();

        reactor.core.Disposable d = containerClient.listBlobs(options)
            .filter(item ->
            {
                if (renderedFilter == Filter.FILES && Boolean.TRUE.equals(item.isPrefix()))
                    return false;
                if (renderedFilter == Filter.DIRECTORY && !Boolean.TRUE.equals(item.isPrefix()))
                    return false;
                if (renderedRegexp != null && !item.getName().matches(renderedRegexp))
                    return false;
                return true;
            })
            .map(item -> Blob.of(containerClient.getBlobContainerName(), item))
            .take(rMaxFiles)
            .subscribe(
                list::add,
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

        runContext.metric(Counter.of("blobs.count", list.size()));

        runContext.logger().debug(
            "Found '{}' keys on {} with regexp='{}', prefix={}",
            list.size(),
            runContext.render(containerClient.getBlobContainerName()),
            runContext.render(regexp).as(String.class).orElse(null),
            runContext.render(prefix).as(String.class).orElse(null)
        );

        return Output.builder()
            .blobs(list)
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The list of blobs"
        )
        private final java.util.List<Blob> blobs;
    }
}
