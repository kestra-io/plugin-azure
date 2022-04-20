package io.kestra.plugin.azure.storage.blob;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.models.BlobProperties;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.storage.blob.abstracts.AbstractBlobStorageObject;
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
            code = {
                "endpoint: \"https://yourblob.blob.core.windows.net\"",
                "connectionString: \"DefaultEndpointsProtocol=...==\"",
                "container: \"mydata\"",
                "name: \"myblob\""
            }
        )
    }
)
@Schema(
    title = "Delete a file from an Azure Blob Storage."
)
public class Delete extends AbstractBlobStorageObject implements RunnableTask<Delete.Output> {
    @Override
    public Output run(RunContext runContext) throws Exception {
        BlobClient blobClient = this.blobClient(runContext);
        BlobProperties properties = blobClient.getProperties();

        blobClient.delete();

        return Output
            .builder()
            .blob(Blob.of(blobClient, properties))
            .build();
    }

    @SuperBuilder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The blob"
        )
        private final Blob blob;
    }
}
