package io.kestra.plugin.azure.storage.blob.services;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
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
import io.kestra.plugin.azure.shared.storage.blob.models.Blob;
import io.kestra.plugin.azure.storage.blob.Copy;
import io.kestra.plugin.azure.storage.blob.Delete;
import io.kestra.plugin.azure.storage.blob.abstracts.ActionInterface;
import io.kestra.plugin.azure.storage.services.ChecksumValidator;

public class BlobService {
    public static Pair<BlobProperties, URI> download(RunContext runContext, BlobClient client) throws IOException {
        return download(runContext, client, null);
    }

    public static Pair<BlobProperties, URI> download(
        RunContext runContext,
        BlobClient client,
        ChecksumValidator.Options checksumOptions) throws IOException {
        File tempFile = runContext.workingDir().createTempFile(FileUtils.getExtension(client.getBlobName())).toFile();
        BlobProperties blobProperties = client.downloadToFile(tempFile.getAbsolutePath(), true);

        runContext.metric(Counter.of("file.size", blobProperties.getBlobSize()));

        ChecksumValidator.verify(
            runContext,
            tempFile,
            blobProperties.getContentMd5(),
            checksumOptions,
            client.getBlobName()
        );

        return Pair.of(blobProperties, runContext.storage().putFile(tempFile));
    }

    public static boolean isDirectoryMarker(Blob blob) {
        return blob != null && isDirectoryMarker(blob.getName());
    }

    public static boolean isDirectoryMarker(String name) {
        return name != null && name.endsWith("/");
    }

    public static List<Pair<Blob, String>> moveTargets(
        List<Blob> blobsObjects,
        ActionInterface.Action action,
        Copy.CopyObject moveTo,
        RunContext runContext,
        Property<String> prefix
    ) throws IllegalVariableEvaluationException {
        if (action != ActionInterface.Action.MOVE || moveTo == null) {
            return List.of();
        }

        var rMoveToName = runContext.render(moveTo.getName()).as(String.class).orElseThrow();
        var rPrefix = prefix != null ? runContext.render(prefix).as(String.class).orElse(null) : null;

        var destinationMap = new HashMap<String, Blob>();
        var targets = new ArrayList<Pair<Blob, String>>();

        for (var object : blobsObjects) {
            if (isDirectoryMarker(object)) {
                continue;
            }
            var targetName = destinationName(rMoveToName, object.getName(), rPrefix);
            var existing = destinationMap.put(targetName, object);
            if (existing != null) {
                throw new IllegalStateException(
                    "Cannot move blobs to '%s': multiple source blobs would overwrite the same destination name: '%s' and '%s'. Set a `prefix` that covers both folders, or narrow the listing with `prefix` or `regexp`, so each blob maps to its own name under `moveTo.name`."
                        .formatted(targetName, existing.getName(), object.getName())
                );
            }
            targets.add(Pair.of(object, targetName));
        }

        return targets;
    }

    public static String destinationName(String moveToName, String objectName, String prefix) {
        var destPrefix = StringUtils.stripEnd(moveToName, "/");
        var subpath = "";

        if (prefix != null) {
            var normPrefix = StringUtils.strip(prefix, "/");
            var normName = StringUtils.stripStart(objectName, "/");
            if (!normPrefix.isEmpty()) {
                if (normName.startsWith(normPrefix + "/")) {
                    subpath = normName.substring(normPrefix.length() + 1);
                } else if (normPrefix.contains("/")) {
                    var folderBase = normPrefix.substring(0, normPrefix.lastIndexOf('/'));
                    if (!folderBase.isEmpty() && normName.startsWith(folderBase + "/")) {
                        subpath = normName.substring(folderBase.length() + 1);
                    }
                }
            }
        }

        if (subpath.isEmpty()) {
            subpath = FilenameUtils.getName(objectName);
        }

        if (subpath.isEmpty()) {
            return destPrefix;
        }
        return destPrefix.isEmpty() ? subpath : destPrefix + "/" + subpath;
    }

    public static void archive(
        List<Blob> blobsObjects,
        ActionInterface.Action action,
        Copy.CopyObject moveTo,
        RunContext runContext,
        AbstractConnectionInterface connectionInterface,
        AzureClientWithSasInterface blobStorageInterface) throws Exception {
        archive(blobsObjects, action, moveTo, runContext, connectionInterface, blobStorageInterface, null);
    }

    public static void archive(
        List<Blob> blobsObjects,
        ActionInterface.Action action,
        Copy.CopyObject moveTo,
        RunContext runContext,
        AbstractConnectionInterface connectionInterface,
        AzureClientWithSasInterface blobStorageInterface,
        Property<String> prefix) throws Exception {
        if (action == ActionInterface.Action.DELETE) {
            for (Blob object : blobsObjects) {
                if (isDirectoryMarker(object)) {
                    continue;
                }
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
            var targets = moveTargets(blobsObjects, action, moveTo, runContext, prefix);

            for (var target : targets) {
                var object = target.getLeft();
                var targetName = target.getRight();
                var targetContainer = moveTo.getContainer() != null ? moveTo.getContainer() : Property.ofValue(object.getContainer());

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
                            .container(targetContainer)
                            .name(Property.ofValue(targetName))
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
        return clientBuilder(endpoint, connectionString, sharedKeyAccountName, sharedKeyAccountAccessKey, sasToken, runContext).buildClient();
    }

    public static com.azure.storage.blob.BlobServiceAsyncClient asyncClient(
        Property<String> endpoint,
        Property<String> connectionString,
        Property<String> sharedKeyAccountName,
        Property<String> sharedKeyAccountAccessKey,
        Property<String> sasToken,
        RunContext runContext) throws IllegalVariableEvaluationException {
        return clientBuilder(endpoint, connectionString, sharedKeyAccountName, sharedKeyAccountAccessKey, sasToken, runContext).buildAsyncClient();
    }

    private static BlobServiceClientBuilder clientBuilder(
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

        return builder;
    }
}
