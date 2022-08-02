package io.kestra.plugin.azure.storage.blob.services;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.ListBlobsOptions;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.AbstractConnectionInterface;
import io.kestra.plugin.azure.storage.abstracts.AbstractStorageInterface;
import io.kestra.plugin.azure.storage.blob.Copy;
import io.kestra.plugin.azure.storage.blob.Delete;
import io.kestra.plugin.azure.storage.blob.abstracts.ActionInterface;
import io.kestra.plugin.azure.storage.blob.abstracts.ListInterface;
import io.kestra.plugin.azure.storage.blob.models.Blob;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

public class BlobService {
    public static Pair<BlobProperties, URI> download(RunContext runContext, BlobClient client) throws IOException {
        File tempFile = runContext.tempFile().toFile();
        BlobProperties blobProperties = client.downloadToFile(tempFile.getAbsolutePath(), true);

        runContext.metric(Counter.of("file.size", blobProperties.getBlobSize()));

        return Pair.of(blobProperties, runContext.putTempFile(tempFile));
    }

    public static void archive(
        List<Blob> blobsObjects,
        ActionInterface.Action action,
        Copy.CopyObject moveTo,
        RunContext runContext,
        AbstractConnectionInterface connectionInterface,
        AbstractStorageInterface blobStorageInterface
    ) throws Exception {
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
                    .container(object.getContainer())
                    .name(object.getName())
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
                    .from(Copy.CopyObject.builder()
                        .container(object.getContainer())
                        .name(object.getName())
                        .build()
                    )
                    .to(moveTo.toBuilder()
                        .container(object.getContainer())
                        .name(StringUtils.stripEnd(moveTo.getName() + "/", "/")
                            + "/" + FilenameUtils.getName(object.getName())
                        )
                        .build()
                    )
                    .delete(true)
                    .build();
                copy.run(runContext);
            }
        }
    }

    public static List<Blob> list(RunContext runContext, BlobContainerClient client, ListInterface list) throws IllegalVariableEvaluationException {
        ListBlobsOptions listBlobsOptions = new ListBlobsOptions();

        if (list.getPrefix() != null) {
            listBlobsOptions.setPrefix(runContext.render(list.getPrefix()));
        }

        String regExp = runContext.render(list.getRegexp());


        PagedIterable<BlobItem> blobItems;
        if (list.getDelimiter() != null) {
            blobItems = client.listBlobsByHierarchy(runContext.render(list.getDelimiter()), listBlobsOptions, Duration.ofSeconds(30));
        } else {
            blobItems = client.listBlobs(listBlobsOptions, Duration.ofSeconds(30));
        }

        return blobItems
            .stream()
            .filter(blob -> BlobService.filter(blob, regExp, list.getFilter()))
            .map(blob -> Blob.of(client.getBlobContainerName(), blob))
            .collect(Collectors.toList());
    }

    private static boolean filter(BlobItem object, String regExp, ListInterface.Filter filter) {
        return (regExp == null || object.getName().matches(regExp)) &&
            (
                (filter == ListInterface.Filter.BOTH) ||
                (filter == ListInterface.Filter.DIRECTORY && object.getProperties().getContentType() == null) ||
                (filter == ListInterface.Filter.FILES && object.getProperties().getContentType() != null)
            );
    }
}
