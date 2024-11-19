package io.kestra.plugin.azure.storage.adls;

import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.storage.adls.abstracts.AbstractDataLakeConnection;
import io.kestra.plugin.azure.storage.adls.abstracts.AbstractDataLakeStorageInterface;
import io.kestra.plugin.azure.storage.adls.models.AdlsFile;
import io.kestra.plugin.azure.storage.adls.services.DataLakeService;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Schedulers;

import java.util.NoSuchElementException;
import java.util.function.Function;

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
                namespace: company.team

                tasks:
                  - id: delete_list
                    type: io.kestra.plugin.azure.storage.blob.DeleteList
                    endpoint: "https://yourblob.blob.core.windows.net"
                    sasToken: "{{ secret('SAS_TOKEN') }}"
                    fileSystem: "fileSystem"
                    directoryName: "path/to/mydirectory"
                """
        )
    }
)
@Schema(
    title = "Delete a list of keys from the Azure Data Lake Storage."
)
public class DeleteFiles extends AbstractDataLakeConnection implements RunnableTask<DeleteFiles.Output>, AbstractDataLakeStorageInterface {
    protected String fileSystem;

    @Schema(title = "Directory Name")
    @PluginProperty(dynamic = true)
    @NotNull
    protected String directoryName;

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

        DataLakeServiceClient client = this.dataLakeServiceClient(runContext);
        DataLakeFileSystemClient fileSystemClient = client.getFileSystemClient(runContext.render(this.fileSystem));

        Flux<AdlsFile> flowable = Flux
            .create(throwConsumer(emitter -> {
                DataLakeService
                    .list(runContext, fileSystemClient, directoryName)
                        .forEach(emitter::next);

                emitter.complete();
            }), FluxSink.OverflowStrategy.BUFFER);

        Flux<Long> result;

        if (this.concurrent != null) {
            result = flowable
                .parallel(this.concurrent)
                .runOn(Schedulers.boundedElastic())
                .map(delete(logger, fileSystemClient))
                .sequential();
        } else {
            result = flowable
                .map(delete(logger, fileSystemClient));
        }

        Pair<Long, Long> finalResult = result
            .reduce(Pair.of(0L, 0L), (pair, size) -> Pair.of(pair.getLeft() + 1, pair.getRight() + size))
            .blockOptional()
            .orElse(Pair.of(0L, 0L));

        runContext.metric(Counter.of("count", finalResult.getLeft()));
        runContext.metric(Counter.of("size", finalResult.getRight()));

        if (Boolean.TRUE.equals(errorOnEmpty) && finalResult.getLeft() == 0) {
            throw new NoSuchElementException("Unable to find any files to delete on " +
                runContext.render(this.fileSystem) + " " +
                "with directoryName='" + runContext.render(this.directoryName)
            );
        }

        logger.info("Deleted {} keys for {} bytes", finalResult.getLeft(), finalResult.getValue());

        return Output
            .builder()
            .count(finalResult.getLeft())
            .size(finalResult.getRight())
            .build();
    }

    private static Function<AdlsFile, Long> delete(Logger logger, DataLakeFileSystemClient fileSystemClient) {
        return o -> {
            logger.debug("Deleting '{}'", o.getName());

            DataLakeFileClient fileClient = fileSystemClient.getFileClient(o.getName());
            long fileSize = fileClient.getProperties().getFileSize();

            fileClient.delete();

            return fileSize;
        };
    }


    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Builder.Default
        @Schema(
            title = "The count of deleted files."
        )
        private final long count = 0;

        @Builder.Default
        @Schema(
            title = "The size of all the deleted files."
        )
        private final long size = 0;
    }
}
