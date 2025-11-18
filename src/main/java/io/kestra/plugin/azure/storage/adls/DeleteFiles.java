package io.kestra.plugin.azure.storage.adls;

import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
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
            title = "Download files from a remote server, upload them to Azure Data Lake Storage, finally delete them all at one",
            code = """
                id: azure_storage_blob_delete_list
                namespace: company.team

                pluginDefaults:
                  - type: io.kestra.plugin.azure.storage.adls
                    values:
                      connectionString: "{{ secret('AZURE_CONNECTION_STRING') }}"
                      fileSystem: "tasks"
                      endpoint: "https://yourblob.blob.core.windows.net"

                tasks:
                  - id: for_each
                    type: io.kestra.plugin.core.flow.EachSequential
                    value: ["pikachu", "charmander"]
                    tasks:
                      - id: download_request
                        type: io.kestra.plugin.core.http.Download
                        uri: https://pokeapi.co/api/v2/pokemon/{{ taskrun.value }}

                      - id: to_ion
                        type: io.kestra.plugin.serdes.json.JsonToIon
                        from: "{{ currentEachOutput(outputs.download_request).uri }}"

                      - id: upload_file
                        type: io.kestra.plugin.azure.storage.adls.Upload
                        fileName: "adls/pokemon/{{ taskrun.value }}.json"
                        from: "{{ currentEachOutput(outputs.to_ion).uri }}"

                  - id: delete_file
                    type: io.kestra.plugin.azure.storage.adls.DeleteFiles
                    concurrent: 2
                    directoryPath: "adls/pokemon/"
                """
        )
    },
    metrics = {
        @Metric(name = "files.count", type = Counter.TYPE, description = "The total number of files deleted."),
        @Metric(name = "files.size", type = Counter.TYPE, description = "The total size of all files deleted, in bytes.")
    }
)
@Schema(
    title = "Delete a list of objects from Azure Data Lake Storage."
)
public class DeleteFiles extends AbstractDataLakeConnection implements RunnableTask<DeleteFiles.Output>, AbstractDataLakeStorageInterface {
    protected Property<String> fileSystem;

    @Schema(title = "Directory Name")
    @NotNull
    protected Property<String> directoryPath;

    @Min(2)
    @Schema(
        title = "Number of concurrent parallel deletions."
    )
    @PluginProperty(dynamic = false)
    private Integer concurrent;

    @Schema(
        title = "Whether to raise an error if the file is not found."
    )
    @Builder.Default
    private final Property<Boolean> errorOnEmpty = Property.ofValue(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        DataLakeServiceClient client = this.dataLakeServiceClient(runContext);
        DataLakeFileSystemClient fileSystemClient = client.getFileSystemClient(runContext.render(this.fileSystem).as(String.class).orElse(null));

        Flux<AdlsFile> flowable = Flux
            .create(throwConsumer(emitter -> {
                DataLakeService
                    .list(fileSystemClient, runContext.render(directoryPath).as(String.class).orElseThrow())
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

        runContext.metric(Counter.of("files.count", finalResult.getLeft()));
        runContext.metric(Counter.of("files.size", finalResult.getRight()));

        if (Boolean.TRUE.equals(runContext.render(errorOnEmpty).as(Boolean.class).orElseThrow()) && finalResult.getLeft() == 0) {
            throw new NoSuchElementException("Unable to find any files to delete on " +
                runContext.render(this.fileSystem).as(String.class).orElse(null) + " " +
                "with directoryPath='" + runContext.render(this.directoryPath).as(String.class).orElseThrow()
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
            logger.info("Deleting '{}'", o.getName());

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
