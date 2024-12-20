package io.kestra.plugin.azure.storage.blob;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobProperties;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.storage.blob.abstracts.AbstractBlobStorageWithSas;
import io.kestra.plugin.azure.storage.blob.abstracts.AbstractBlobStorageContainerInterface;
import io.kestra.plugin.azure.storage.blob.abstracts.ActionInterface;
import io.kestra.plugin.azure.storage.blob.abstracts.ListInterface;
import io.kestra.plugin.azure.storage.blob.models.Blob;
import io.kestra.plugin.azure.storage.blob.services.BlobService;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.tuple.Pair;

import java.net.URI;
import java.util.AbstractMap;
import java.util.Map;
import java.util.stream.Collectors;

import static io.kestra.core.utils.Rethrow.throwFunction;

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
                id: azure_storage_blob_downloads
                namespace: company.team

                tasks:
                  - id: downloads
                    type: io.kestra.plugin.azure.storage.blob.Downloads
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
    title = "Downloads files from the Azure Blob Storage."
)
public class Downloads extends AbstractBlobStorageWithSas implements RunnableTask<Downloads.Output>, ListInterface, ActionInterface, AbstractBlobStorageContainerInterface {
    private Property<String> container;

    private Property<String> prefix;

    protected Property<String> regexp;

    protected Property<String> delimiter;

    private Property<ActionInterface.Action> action;

    private Copy.CopyObject moveTo;

    @Builder.Default
    private Property<Filter> filter = Property.of(Filter.FILES);

    @Override
    public Output run(RunContext runContext) throws Exception {
        List task = List.builder()
            .id(this.id)
            .type(List.class.getName())
            .endpoint(this.endpoint)
            .connectionString(this.connectionString)
            .sharedKeyAccountName(this.sharedKeyAccountName)
            .sharedKeyAccountAccessKey(this.sharedKeyAccountAccessKey)
            .sasToken(this.sasToken)
            .container(this.container)
            .prefix(this.prefix)
            .delimiter(this.delimiter)
            .regexp(this.regexp)
            .delimiter(this.delimiter)
            .build();
        List.Output run = task.run(runContext);

        BlobServiceClient client = this.client(runContext);
        BlobContainerClient containerClient = client.getBlobContainerClient(runContext.render(this.container).as(String.class).orElse(null));

        java.util.List<Blob> list = run
            .getBlobs()
            .stream()
            .map(throwFunction(object -> {
                BlobClient blobClient = containerClient.getBlobClient(object.getName());

                Pair<BlobProperties, URI> download = BlobService.download(runContext, blobClient);

                return Blob.of(blobClient, download.getLeft())
                    .withUri(download.getRight());
            }))
            .collect(Collectors.toList());

        Map<String, URI> outputFiles = list.stream()
            .filter(blob -> !blob.getName().endsWith("/"))
            .map(blob -> new AbstractMap.SimpleEntry<>(blob.getName(), blob.getUri()))
            .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue()));

        BlobService.archive(
            run.getBlobs(),
            runContext.render(this.action).as(ActionInterface.Action.class).orElseThrow(),
            this.moveTo,
            runContext,
            this,
            this
        );

        return Output
            .builder()
            .blobs(list)
            .outputFiles(outputFiles)
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The list of blobs."
        )
        private final java.util.List<Blob> blobs;

        @Schema(
            title = "The downloaded files as a map of from/to URIs."
        )
        private final Map<String, URI> outputFiles;
    }
}
