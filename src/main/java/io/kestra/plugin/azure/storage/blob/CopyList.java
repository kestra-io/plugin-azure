package io.kestra.plugin.azure.storage.blob;

import java.time.OffsetDateTime;

import com.azure.core.util.polling.LongRunningOperationStatus;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;

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
import io.kestra.plugin.azure.shared.storage.blob.services.BlobService;
import io.kestra.plugin.azure.storage.blob.models.CopyListDestination;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.tuple.Pair;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

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
                    connectionString: "{{ secret('AZURE_STORAGE_CONNECTION_STRING') }}"
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
    description = "Lists blobs matching the prefix, regexp and filter, and copies them server-side to a destination container and prefix. "
        + "Authentication must provide an account key to sign a read SAS on each source blob: use `connectionString` containing `AccountKey=`, "
        + "or both `sharedKeyAccountName` and `sharedKeyAccountAccessKey`. `sasToken`, a connection string carrying only a SAS, and default credentials are not supported. "
        + "When copying inside the same container, `to.prefix` must differ from `prefix` and must not be nested under it unless a `delimiter` is set."
)
public class CopyList extends AbstractBlobStorageWithSas implements RunnableTask<CopyList.Output>, ListInterface, AbstractBlobStorageContainerInterface {
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

    @NotNull
    @Valid
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
    @Max(64)
    @Schema(
        title = "Parallel copies",
        description = "Maximum number of concurrent copy operations, between 2 and 64. Blobs are copied sequentially when not set."
    )
    @PluginProperty(group = "execution")
    private Property<Integer> concurrent;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rFromContainer = runContext.render(this.container).as(String.class).orElseThrow();
        var rFromPrefix = runContext.render(this.prefix).as(String.class).filter(p -> !p.isEmpty()).orElse(null);
        var rToContainer = runContext.render(this.to.getContainer()).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("`to.container` is required: set the destination container"));
        var rToPrefix = runContext.render(this.to.getPrefix()).as(String.class).filter(p -> !p.isEmpty()).orElse(null);
        var rDelete = runContext.render(this.delete).as(Boolean.class).orElse(false);
        var rConcurrent = runContext.render(this.concurrent).as(Integer.class).orElse(null);

        if (rConcurrent != null && (rConcurrent < 2 || rConcurrent > 64)) {
            throw new IllegalArgumentException("`concurrent` must be between 2 and 64, got " + rConcurrent);
        }

        if (rToContainer.equals(rFromContainer)) {
            if (rToPrefix == null || rToPrefix.equals(rFromPrefix)) {
                throw new IllegalArgumentException(
                    "Source and destination resolve to the same blobs; set a different `to.container` or `to.prefix`"
                );
            }
            var hasDelimiter = runContext.render(this.delimiter).as(String.class).isPresent();
            if (!hasDelimiter && (rFromPrefix == null || rToPrefix.startsWith(rFromPrefix))) {
                throw new IllegalArgumentException(
                    "`to.prefix` '" + rToPrefix + "' is inside the listed source range, so copied blobs would be listed and copied again; "
                        + "set a `to.prefix` outside `prefix`, a different `to.container`, or a `delimiter`"
                );
            }
        }

        // Copy needs a read SAS for the source, which can only be signed with a shared key
        var rConnectionString = runContext.render(this.getConnectionString()).as(String.class).orElse("");
        var hasSharedKey = rConnectionString.contains("AccountKey=")
            || rConnectionString.contains("UseDevelopmentStorage=true")
            || (runContext.render(this.getSharedKeyAccountName()).as(String.class).isPresent()
                && runContext.render(this.getSharedKeyAccountAccessKey()).as(String.class).isPresent());
        if (!hasSharedKey) {
            throw new IllegalArgumentException(
                "CopyList requires an account key to sign a source read SAS: set `connectionString` with `AccountKey=`, or both `sharedKeyAccountName` and `sharedKeyAccountAccessKey`; `sasToken`, a SAS-only connection string and default credentials are not supported"
            );
        }

        var client = this.client(runContext);
        var sourceContainerClient = client.getBlobContainerClient(rFromContainer);
        var destContainerClient = client.getBlobContainerClient(rToContainer);

        var blobs = Flux.fromIterable(BlobService.list(runContext, sourceContainerClient, this));

        Flux<Long> copied;
        if (rConcurrent != null) {
            copied = blobs
                .parallel(rConcurrent)
                .runOn(Schedulers.boundedElastic())
                .map(blob -> copy(blob, sourceContainerClient, destContainerClient, rFromPrefix, rToPrefix, rDelete))
                .sequential();
        } else {
            copied = blobs.map(blob -> copy(blob, sourceContainerClient, destContainerClient, rFromPrefix, rToPrefix, rDelete));
        }

        var result = copied
            .filter(size -> size != SKIPPED)
            .reduce(Pair.of(0L, 0L), (pair, size) -> Pair.of(pair.getLeft() + 1, pair.getRight() + size))
            .block();

        runContext.metric(Counter.of("azure.blob.copied.count", result.getLeft()));
        runContext.metric(Counter.of("azure.blob.copied.size", result.getRight()));

        runContext.logger().info("Copied {} blobs for {} bytes", result.getLeft(), result.getRight());

        return Output.builder()
            .count(result.getLeft())
            .size(result.getRight())
            .build();
    }

    static String destinationName(String name, String fromPrefix, String toPrefix) {
        if (toPrefix == null) {
            return name;
        }
        return fromPrefix != null && name.startsWith(fromPrefix)
            ? toPrefix + name.substring(fromPrefix.length())
            : toPrefix + name;
    }

    private static final long SKIPPED = -1L;

    private static long copy(
        Blob blob,
        BlobContainerClient source,
        BlobContainerClient dest,
        String fromPrefix,
        String toPrefix,
        boolean deleteSource
    ) {
        var destName = destinationName(blob.getName(), fromPrefix, toPrefix);

        // Never copy a blob onto itself: with delete enabled it would then be deleted
        if (source.getBlobContainerName().equals(dest.getBlobContainerName()) && destName.equals(blob.getName())) {
            return SKIPPED;
        }

        var sourceBlob = source.getBlobClient(blob.getName());
        var destBlob = dest.getBlobClient(destName);

        // Short-lived read SAS so the destination request is allowed to read a private source
        var values = new BlobServiceSasSignatureValues(
            OffsetDateTime.now().plusMinutes(15),
            new BlobSasPermission().setReadPermission(true)
        ).setStartTime(OffsetDateTime.now());
        var sourceUrl = sourceBlob.getBlobUrl() + "?" + sourceBlob.generateSas(values);

        try {
            var response = destBlob.beginCopy(sourceUrl, null).waitForCompletion();

            if (response.getStatus() != LongRunningOperationStatus.SUCCESSFULLY_COMPLETED) {
                throw new IllegalStateException(
                    "Copy of blob '" + blob.getName() + "' from container '" + source.getBlobContainerName() +
                        "' to '" + dest.getBlobContainerName() + "/" + destName + "' ended with status " + response.getStatus() +
                        ", the source was not deleted"
                );
            }
        } catch (BlobStorageException e) {
            throw new IllegalStateException(
                "Failed to copy blob '" + blob.getName() + "' from container '" + source.getBlobContainerName() +
                    "' to '" + dest.getBlobContainerName() + "/" + destName + "' (Azure error code: " + e.getErrorCode() +
                    "). Check that the credentials can read the source and write the destination.",
                e
            );
        }

        if (deleteSource) {
            sourceBlob.delete();
        }

        return blob.getSize() == null ? 0L : blob.getSize();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Number of blobs copied")
        private final long count;

        @Schema(title = "Total size in bytes of copied blobs")
        private final long size;
    }
}
