package io.kestra.plugin.azure.storage.adls;

import com.azure.core.util.BinaryData;
import com.azure.core.util.FluxUtil;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.specialized.BlobLeaseClient;
import com.azure.storage.blob.specialized.BlobLeaseClientBuilder;
import com.azure.storage.common.implementation.Constants;
import com.azure.storage.file.datalake.DataLakeFileClient;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.storage.adls.abstracts.AbstractDataLakeWithFile;
import io.kestra.plugin.azure.storage.adls.models.AdlsFile;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import reactor.core.scheduler.Schedulers;

import java.io.InputStream;
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
            title = "Download a json file and upload it to Azure Data Lake Storage.",
            code = """
                id: azure_data_lake_storage_upload
                namespace: company.team

                tasks:
                  - id: download_request
                    type: io.kestra.plugin.core.http.Download
                    uri: adls/product_data/product.json

                  - id: upload_file
                    type: io.kestra.plugin.azure.storage.adls.Upload
                    filePath: "path/to/file/product.json"
                    from: "{{ outputs.download_request.uri }}"
                    connectionString: "{{ secret('AZURE_CONNECTION_STRING') }}"
                    fileSystem: "tasks"
                    endpoint: "https://yourblob.blob.core.windows.net"
                """
        )
    },
    metrics = {
        @Metric(name = "file.size", type = Counter.TYPE, description = "The size of the uploaded file, in bytes.")
    }
)
@Schema(
    title = "Upload a file to Azure Data Lake Storage."
)
public class Upload extends AbstractDataLakeWithFile implements RunnableTask<Upload.Output> {

    public static final int AZURE_LEASE_MIN_DURATION = 15;
    public static final int AZURE_LEASE_MAX_DURATION = 60;

    @Schema(
        title = "The file from the internal storage to upload to the Azure Data Lake Storage."
    )
    @PluginProperty(internalStorageURI = true)
    @NotNull
    private Property<String> from;

    @Schema(title = "Enable blob lease before upload to prevent concurrent writes.")
    private Property<Boolean> useLease;

    @Schema(title = "Lease duration in seconds (between 15 and 60).")
    private Property<Integer> leaseDurationSeconds;

    @Override
    public Upload.Output run(RunContext runContext) throws Exception {
        URI fromUri = new URI(runContext.render(this.from).as(String.class).orElseThrow());

        try (InputStream is = runContext.storage().getFile(fromUri)) {
            DataLakeFileClient fileClient = this.dataLakeFileClient(runContext);

            boolean enableLease = runContext.render(this.useLease).as(Boolean.class).orElse(false);
            int leaseDuration = Math.max(
                AZURE_LEASE_MIN_DURATION,
                Math.min(
                    runContext.render(this.leaseDurationSeconds).as(Integer.class).orElse(AZURE_LEASE_MIN_DURATION),
                    AZURE_LEASE_MAX_DURATION
                )
            );

            String leaseId = null;
            BlobLeaseClient leaseClient = null;
            BlobClient blobClient = null;

            String filePath = fileClient.getFilePath();
            if (enableLease) {
                String endpoint = runContext.render(this.getEndpoint()).as(String.class).orElseThrow();
                String fileSystem = runContext.render(this.getFileSystem()).as(String.class).orElseThrow();
                String connectionString = runContext.render(this.getConnectionString()).as(String.class).orElseThrow();

                BlobServiceClient blobServiceClient =
                    new BlobServiceClientBuilder()
                        .connectionString(connectionString)
                        .endpoint(endpoint)
                        .buildClient();

                BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(fileSystem);
                blobClient = containerClient.getBlobClient(filePath);

                leaseClient = new BlobLeaseClientBuilder()
                    .blobClient(blobClient)
                    .buildClient();

                try {
                    leaseId = leaseClient.acquireLease(leaseDuration);
                    runContext.logger().debug("Acquired lease {} on file {}", leaseId, filePath);
                } catch (Exception e) {
                    runContext.logger().warn("Failed to acquire lease on {}: {}", filePath, e.getMessage());
                }
            }

            try {
                BinaryData binaryData = BinaryData.fromFlux(
                    FluxUtil.toFluxByteBuffer(is, Constants.MAX_INPUT_STREAM_CONVERTER_BUFFER_LENGTH)
                        .subscribeOn(Schedulers.boundedElastic())
                ).block();

                if (enableLease && leaseId != null && blobClient != null && binaryData != null) {
                    blobClient.upload(binaryData.toStream(), true);
                    runContext.logger().debug("Uploaded file {} using blobClient under lease {}", filePath, leaseId);
                } else {
                    fileClient.upload(binaryData, true);
                    runContext.logger().debug("Uploaded file {} using fileClient (no lease)", filePath);
                }

                runContext.metric(Counter.of("file.size", fileClient.getProperties().getFileSize()));

            } finally {
                if (leaseClient != null && leaseId != null) {
                    try {
                        leaseClient.releaseLease();
                        runContext.logger().debug("Released lease {} on {}", leaseId, filePath);
                    } catch (Exception e) {
                        runContext.logger().warn("Failed to release lease {} on {}", leaseId, filePath, e);
                    }
                }
            }

            return Output.builder()
                .file(AdlsFile.of(fileClient))
                .build();
        }
    }

    @SuperBuilder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The uploaded file."
        )
        private final AdlsFile file;
    }
}
