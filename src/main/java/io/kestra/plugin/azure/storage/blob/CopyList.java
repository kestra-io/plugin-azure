package io.kestra.plugin.azure.storage.blob;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobItem;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.storage.blob.models.Blob;
import io.kestra.plugin.azure.storage.blob.models.CopyListDestination;
import io.kestra.plugin.azure.storage.blob.models.CopyListSource;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            title = "Batch copy blobs from one prefix to another container",
            full = true,
            code = """
                id: azure_storage_blob_copy_list
                namespace: company.team
                tasks:
                  - id: copy_blobs
                    type: io.kestra.plugin.azure.storage.blob.CopyList
                    endpoint: "https://yourblob.blob.core.windows.net"
                    connectionString: "DefaultEndpointsProtocol=...=="
                    from:
                      container: "staging"
                      prefix: "daily-export/"
                    to:
                      container: "production"
                      prefix: "daily-export/"
                    delete: false
                """
        )
    },
    metrics = {
        @Metric(name = "azure.blob.copied.count", type = Counter.TYPE, unit = "blobs", description = "The total number of blobs copied."),
        @Metric(name = "azure.blob.copied.size", type = Counter.TYPE, unit = "bytes", description = "The total size in bytes of blobs copied.")
    }
)
@Schema(
    title = "Copy multiple Azure Blobs",
    description = "Lists blobs matching prefix/regex filters and copies them server-side to a destination container/prefix."
)
public class CopyList extends AbstractBlobStorageWithSas implements RunnableTask<CopyList.Output> {

    @NotNull
    @Schema(title = "Source configuration (container, prefix, regex filter)")
    @PluginProperty(group = "source")
    private CopyListSource from;

    @NotNull
    @Schema(title = "Destination configuration (container, prefix)")
    @PluginProperty(group = "destination")
    private CopyListDestination to;

    @Schema(
        title = "Delete source blobs after copy",
        description = "If true, deletes each source blob after it has been successfully copied (move operation)."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Boolean> delete = Property.ofValue(false);

    @Min(2)
    @Schema(title = "Parallel copies", description = "Max concurrent copy operations.")
    @PluginProperty(group = "execution")
    private Integer concurrent;

    @Override
    public Output run(RunContext runContext) throws Exception {
        BlobServiceClient client = this.client(runContext);
        
        String fromContainer = runContext.render(this.from.getContainer());
        String fromPrefix = runContext.render(this.from.getPrefix());
        String fromRegexp = runContext.render(this.from.getRegexp());
        
        String toContainer = runContext.render(this.to.getContainer());
        String toPrefix = runContext.render(this.to.getPrefix());
        boolean deleteSource = runContext.render(this.delete).as(Boolean.class).orElse(false);
        int concurrency = this.concurrent != null ? this.concurrent : 1;

        BlobContainerClient sourceContainerClient = client.getBlobContainerClient(fromContainer);
        BlobContainerClient destContainerClient = client.getBlobContainerClient(toContainer);

        Pattern pattern = fromRegexp != null ? Pattern.compile(fromRegexp) : null;

        List<Blob> copiedBlobs = Flux.fromIterable(sourceContainerClient.listBlobsByHierarchy(null, null, fromPrefix))
            .filter(BlobItem::isPrefix) // Exclude directories if any
            .map(BlobItem::getName)
            .filter(name -> pattern == null || pattern.matcher(name).matches())
            .parallel(concurrency)
            .runOn(Schedulers.boundedElastic())
            .map(blobName -> {
                try {
                    BlobClient sourceBlob = sourceContainerClient.getBlobClient(blobName);
                    
                    // Determine new blob name
                    String destBlobName = blobName;
                    if (fromPrefix != null && toPrefix != null && blobName.startsWith(fromPrefix)) {
                        destBlobName = toPrefix + blobName.substring(fromPrefix.length());
                    } else if (toPrefix != null) {
                        destBlobName = toPrefix + blobName;
                    }

                    BlobClient destBlob = destContainerClient.getBlobClient(destBlobName);

                    // Initiate server-side copy
                    destBlob.beginCopy(sourceBlob.getBlobUrl(), null).waitForCompletion();

                    long size = sourceBlob.getProperties().getBlobSize();

                    // Delete if requested
                    if (deleteSource) {
                        sourceBlob.delete();
                    }

                    return Blob.of(destBlob, destBlob.getProperties(), destContainerClient.getBlobContainerName());
                } catch (Exception e) {
                    throw new RuntimeException("Failed to copy blob: " + blobName, e);
                }
            })
            .sequential()
            .collectList()
            .block();

        long count = copiedBlobs != null ? copiedBlobs.size() : 0;
        long size = copiedBlobs != null ? copiedBlobs.stream().mapToLong(Blob::getSize).sum() : 0;

        runContext.metric(Counter.of("azure.blob.copied.count", count));
        runContext.metric(Counter.of("azure.blob.copied.size", size));

        return Output.builder()
            .count(count)
            .size(size)
            .blobs(copiedBlobs)
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Number of blobs copied")
        private final long count;

        @Schema(title = "Total size in bytes of copied blobs")
        private final long size;

        @Schema(title = "List of copied blobs output info")
        private final List<Blob> blobs;
    }
}