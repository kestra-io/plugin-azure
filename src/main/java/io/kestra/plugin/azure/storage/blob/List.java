package io.kestra.plugin.azure.storage.blob;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
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
                id: azure_storage_blob_list
                namespace: company.team

                tasks:
                  - id: list
                    type: io.kestra.plugin.azure.storage.blob.List
                    endpoint: "https://yourblob.blob.core.windows.net"
                    connectionString: "DefaultEndpointsProtocol=...=="
                    container: "mydata"
                    prefix: "sub-dir"
                    delimiter: "/"
                """
        )
    },
    metrics = {
        @Metric(name = "blobs.count", type = Counter.TYPE, description = "The total number of blobs listed.")
    }
)
@Schema(
    title = "List blob objects in an Azure Blob Storage container."
)
public class List extends AbstractBlobStorageWithSas implements RunnableTask<List.Output>, ListInterface, AbstractBlobStorageContainerInterface {
    private Property<String> container;

    private Property<String> prefix;

    protected Property<String> regexp;

    protected Property<String> delimiter;

    @Builder.Default
    private Property<Filter> filter = Property.ofValue(Filter.FILES);

    @Schema(
        title = "The maximum number of files to return",
        description = "Limits the number of blobs returned by the list operation. If not specified, all matching blobs will be returned."
    )
    private Property<Integer> maxFiles;

    @Override
    public Output run(RunContext runContext) throws Exception {
        BlobServiceClient client = this.client(runContext);
        BlobContainerClient containerClient = client
                .getBlobContainerClient(runContext.render(this.container).as(String.class).orElse(null));

        java.util.List<Blob> list = BlobService.list(runContext, containerClient, this);

        runContext.metric(Counter.of("blobs.count", list.size()));

        runContext.logger().debug(
                "Found '{}' keys on {} with regexp='{}', prefix={}",
                list.size(),
                runContext.render(containerClient.getBlobContainerName()),
                runContext.render(regexp).as(String.class).orElse(null),
                runContext.render(prefix).as(String.class).orElse(null));

        if (this.maxFiles != null) {
            int rMaxFiles = runContext.render(this.maxFiles).as(Integer.class).orElseThrow();
            if (list.size() > rMaxFiles) {
                runContext.logger().warn(
                        "Listing returned {} blobs but maxFiles limit is {}. Only the first {} blobs will be returned. "
                                +
                                "Increase the maxFiles property if you need more blobs.",
                        list.size(), rMaxFiles, rMaxFiles);
                list = list.subList(0, rMaxFiles);
            }
        }

        return Output.builder()
                .blobs(list)
                .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The list of blobs."
        )
        private final java.util.List<Blob> blobs;
    }
}
