package io.kestra.plugin.azure.storage.blob.services;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.azure.core.credential.AzureNamedKeyCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobProperties;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.FileUtils;
import io.kestra.plugin.azure.shared.AbstractConnectionInterface;
import io.kestra.plugin.azure.shared.AzureClientWithSasInterface;
import io.kestra.plugin.azure.storage.blob.Copy;
import io.kestra.plugin.azure.storage.blob.Delete;
import io.kestra.plugin.azure.storage.blob.abstracts.ActionInterface;
import io.kestra.plugin.azure.shared.storage.blob.models.Blob;

public class BlobService {
    public static Pair<BlobProperties, URI> download(RunContext runContext, BlobClient client) throws IOException {
        File tempFile = runContext.workingDir().createTempFile(FileUtils.getExtension(client.getBlobName())).toFile();
        BlobProperties blobProperties = client.downloadToFile(tempFile.getAbsolutePath(), true);

        runContext.metric(Counter.of("file.size", blobProperties.getBlobSize()));

        return Pair.of(blobProperties, runContext.storage().putFile(tempFile));
    }

    public static void archive(
        List<Blob> blobsObjects,
        ActionInterface.Action action,
        Copy.CopyObject moveTo,
        RunContext runContext,
        AbstractConnectionInterface connectionInterface,
        AzureClientWithSasInterface blobStorageInterface) throws Exception {
        if (action == ActionInterface.Action.DELETE) {
            for (Blob object : blobsObjects) {
                Delete delete = Delete.builder()
                    .id("archive")
                    .type(Delete.class.getName())
                    .endpoint(connectionInterface.getEndpoint())
                    .connectionString(blobStorageInterface.getConnectionString())
                    .sharedKeyAccountName(blobStorageInterface.getSharedKeyAccountName())
                    .sharedKeyAccountAccessKey(blobStorageInterface.getSharedKeyAccountAccessKey())
                    .sasToken(blobStorageInterface.getSasToken())
                    .container(Property.ofValue(object.getContainer()))
                    .name(Property.ofValue(object.getName()))
                    .build();
                delete.run(runContext);
            }
        } else if (action == ActionInterface.Action.MOVE) {
            for (Blob object : blobsObjects) {
                Copy copy = Copy.builder()
                    .id("archive")
                    .type(Copy.class.getName())
                    .endpoint(connectionInterface.getEndpoint())
                    .connectionString(blobStorageInterface.getConnectionString())
                    .sharedKeyAccountName(blobStorageInterface.getSharedKeyAccountName())
                    .sharedKeyAccountAccessKey(blobStorageInterface.getSharedKeyAccountAccessKey())
                    .sasToken(blobStorageInterface.getSasToken())
                    .from(
                        Copy.CopyObject.builder()
                            .container(Property.ofValue(object.getContainer()))
                            .name(Property.ofValue(object.getName()))
                            .build()
                    )
                    .to(
                        moveTo.toBuilder()
                            .container(Property.ofValue(object.getContainer()))
                            .name(
                                Property.ofValue(
                                    StringUtils.stripEnd(runContext.render(moveTo.getName()).as(String.class).orElseThrow() + "/", "/")
                                        + "/" + FilenameUtils.getName(object.getName())
                                )
                            )
                            .build()
                    )
                    .delete(Property.ofValue(true))
                    .build();
                copy.run(runContext);
            }
        }
    }

    public static BlobServiceClient client(
        Property<String> endpoint,
        Property<String> connectionString,
        Property<String> sharedKeyAccountName,
        Property<String> sharedKeyAccountAccessKey,
        Property<String> sasToken,
        RunContext runContext) throws IllegalVariableEvaluationException {
        BlobServiceClientBuilder builder = new BlobServiceClientBuilder();

        String renderedEndpoint = endpoint != null ? runContext.render(endpoint).as(String.class).orElse(null) : null;
        String renderedConnectionString = connectionString != null ? runContext.render(connectionString).as(String.class).orElse(null) : null;
        String renderedSharedKeyAccountName = sharedKeyAccountName != null ? runContext.render(sharedKeyAccountName).as(String.class).orElse(null) : null;
        String renderedSharedKeyAccountAccessKey = sharedKeyAccountAccessKey != null ? runContext.render(sharedKeyAccountAccessKey).as(String.class).orElse(null) : null;
        String renderedSasToken = sasToken != null ? runContext.render(sasToken).as(String.class).orElse(null) : null;

        if (renderedEndpoint != null) {
            builder.endpoint(renderedEndpoint);
        }

        if (renderedConnectionString != null) {
            builder.connectionString(renderedConnectionString);
        } else if (renderedSharedKeyAccountName != null && renderedSharedKeyAccountAccessKey != null) {
            builder.credential(
                new AzureNamedKeyCredential(
                    renderedSharedKeyAccountName,
                    renderedSharedKeyAccountAccessKey
                )
            );
        } else if (renderedSasToken != null) {
            builder.sasToken(renderedSasToken);
        } else {
            builder.credential(new DefaultAzureCredentialBuilder().build());
        }

        return builder.buildClient();
    }
}
