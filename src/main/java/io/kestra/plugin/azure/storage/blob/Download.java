package io.kestra.plugin.azure.storage.blob;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.models.BlobProperties;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.storage.blob.abstracts.AbstractBlobStorageWithSasObject;
import io.kestra.plugin.azure.storage.blob.models.Blob;
import io.kestra.plugin.azure.storage.blob.services.BlobService;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.tuple.Pair;

import java.net.URI;

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
        )
    }
)
@Schema(
    title = "Download a blob to Kestra storage",
    description = "Fetches a blob and stores it in internal storage, returning metadata and the downloaded URI."
)
public class Download extends AbstractBlobStorageWithSasObject implements RunnableTask<Download.Output> {
    @Override
    public Output run(RunContext runContext) throws Exception {
        BlobClient blobClient = this.blobClient(runContext);
        Pair<BlobProperties, URI> download = BlobService.download(runContext, blobClient);

        return Output
            .builder()
            .blob(Blob.of(blobClient, download.getLeft())
                .withUri(download.getRight())
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
