package io.kestra.plugin.azure.storage.blob;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.storage.blob.abstracts.AbstractBlobStorageWithSas;
import io.kestra.plugin.azure.storage.blob.abstracts.AbstractBlobStorageContainerInterface;
import io.kestra.plugin.azure.storage.blob.abstracts.ListInterface;
import io.kestra.plugin.azure.storage.blob.models.Blob;
import io.kestra.plugin.azure.storage.blob.services.BlobService;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import java.util.NoSuchElementException;
import java.util.function.Function;

import jakarta.validation.constraints.Min;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Schedulers;

import static io.kestra.core.utils.Rethrow.throwConsumer;

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
                id: azure_storage_blob_delete_list
                namespace: company.name

                tasks:
                  - id: delete_list
                    type: io.kestra.plugin.azure.storage.blob.DeleteList
                    endpoint: "https://yourblob.blob.core.windows.net"
                    connectionString: "DefaultEndpointsProtocol=...=="
                    container: "mydata"
                    prefix: "sub-dir"
                    delimiter: "/"
                """
        )
    }
)
@Schema(
    title = "Delete a list of keys from the Azure Blob Storage."
)
public class DeleteList extends AbstractBlobStorageWithSas implements RunnableTask<DeleteList.Output>, ListInterface, AbstractBlobStorageContainerInterface {
    private String container;

    private String prefix;

    protected String regexp;

    protected String delimiter;

    @Builder.Default
    private Filter filter = Filter.FILES;

    @Min(2)
    @Schema(
        title = "Number of concurrent parallel deletions."
    )
    @PluginProperty(dynamic = false)
    private Integer concurrent;

    @Schema(
        title = "Whether to raise an error if the file is not found."
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    private final Boolean errorOnEmpty = false;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        BlobServiceClient client = this.client(runContext);
        BlobContainerClient containerClient = client.getBlobContainerClient(runContext.render(this.container));

        Flux<Blob> flowable = Flux
            .create(throwConsumer(emitter -> {
                BlobService
                    .list(runContext, containerClient, this)
                        .forEach(emitter::next);

                emitter.complete();
            }), FluxSink.OverflowStrategy.BUFFER);

        Flux<Long> result;

        if (this.concurrent != null) {
            result = flowable
                .parallel(this.concurrent)
                .runOn(Schedulers.boundedElastic())
                .map(delete(logger, containerClient))
                .sequential();
        } else {
            result = flowable
                .map(delete(logger, containerClient));
        }

        Pair<Long, Long> finalResult = result
            .reduce(Pair.of(0L, 0L), (pair, size) -> Pair.of(pair.getLeft() + 1, pair.getRight() + size))
            .block();

        runContext.metric(Counter.of("count", finalResult.getLeft()));
        runContext.metric(Counter.of("size", finalResult.getRight()));

        if (errorOnEmpty && finalResult.getLeft() == 0) {
            throw new NoSuchElementException("Unable to find any files to delete on " +
                runContext.render(this.container) + " " +
                "with regexp='" + runContext.render(this.regexp) + "', " +
                "prefix='" + runContext.render(this.prefix) + "'"
            );
        }

        logger.info("Deleted {} keys for {} bytes", finalResult.getLeft(), finalResult.getValue());

        return Output
            .builder()
            .count(finalResult.getLeft())
            .size(finalResult.getRight())
            .build();
    }

    private static Function<Blob, Long> delete(Logger logger, BlobContainerClient containerClient) {
        return o -> {
            logger.debug("Deleting '{}'", o.getName());

            BlobClient blobClient = containerClient.getBlobClient(o.getName());
            long blobSize = blobClient.getProperties().getBlobSize();

            blobClient.delete();

            return blobSize;
        };
    }


    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Builder.Default
        @Schema(
            title = "The count of deleted blobs."
        )
        private final long count = 0;

        @Builder.Default
        @Schema(
            title = "The size of all the deleted blobs."
        )
        private final long size = 0;
    }
}
