package io.kestra.plugin.azure.storage.blob;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobProperties;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.storage.abstracts.AbstractStorage;
import io.kestra.plugin.azure.storage.blob.abstracts.AbstractBlobStorage;
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
            code = {
                "endpoint: \"https://yourblob.blob.core.windows.net\"",
                "connectionString: \"DefaultEndpointsProtocol=...==\"",
                "container: \"mydata\"",
                "name: \"myblob\"",
                "prefix: \"sub-dir\"",
                "delimiter: \"/\""
            }
        )
    }
)
@Schema(
    title = "Downloads files from an Azure Blob Storage."
)
public class Downloads extends AbstractBlobStorage implements RunnableTask<List.Output>, ListInterface, ActionInterface, AbstractBlobStorageContainerInterface {
    private String container;

    private String prefix;

    protected String regexp;

    protected String delimiter;

    private ActionInterface.Action action;

    private Copy.CopyObject moveTo;

    @Builder.Default
    private Filter filter = Filter.FILES;

    @Override
    public List.Output run(RunContext runContext) throws Exception {
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
        BlobContainerClient containerClient = client.getBlobContainerClient(runContext.render(this.container));

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

        BlobService.archive(
            run.getBlobs(),
            this.action,
            this.moveTo,
            runContext,
            this,
            this
        );

        return List.Output
            .builder()
            .blobs(list)
            .build();
    }
}
