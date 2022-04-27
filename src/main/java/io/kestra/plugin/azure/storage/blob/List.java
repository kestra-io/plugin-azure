package io.kestra.plugin.azure.storage.blob;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.storage.abstracts.AbstractStorage;
import io.kestra.plugin.azure.storage.blob.abstracts.AbstractBlobStorage;
import io.kestra.plugin.azure.storage.blob.abstracts.AbstractBlobStorageContainerInterface;
import io.kestra.plugin.azure.storage.blob.abstracts.ListInterface;
import io.kestra.plugin.azure.storage.blob.models.Blob;
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
    title = "List blobs on an Azure Blob Storage."
)
public class List extends AbstractBlobStorage implements RunnableTask<List.Output>, ListInterface, AbstractBlobStorageContainerInterface {
    private String container;

    private String prefix;

    protected String regexp;

    protected String delimiter;

    @Builder.Default
    private Filter filter = Filter.FILES;

    @Override
    public Output run(RunContext runContext) throws Exception {
        BlobServiceClient client = this.client(runContext);
        BlobContainerClient containerClient = client.getBlobContainerClient(runContext.render(this.container));

        java.util.List<Blob> list = BlobService.list(runContext, containerClient, this);

        runContext.metric(Counter.of("size", list.size()));

        runContext.logger().debug(
            "Found '{}' keys on {} with regexp='{}', prefix={}",
            list.size(),
            runContext.render(containerClient.getBlobContainerName()),
            runContext.render(regexp),
            runContext.render(prefix)
        );

        return Output.builder()
            .blobs(list)
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The list of objects"
        )
        private final java.util.List<Blob> blobs;
    }
}
