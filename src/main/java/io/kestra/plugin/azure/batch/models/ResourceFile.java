package io.kestra.plugin.azure.batch.models;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Value;

@Builder
@Value
public class ResourceFile {
    @Schema(
        title = "The storage container name in the auto storage Account.",
        description = "The `autoStorageContainerName`, `storageContainerUrl` and `httpUrl` properties are mutually exclusive, " +
            "and one of them must be specified."
    )
    Property<String> autoStorageContainerName;

    @Schema(
        title = "The URL of the blob container within Azure Blob Storage.",
        description = "The `autoStorageContainerName`, `storageContainerUrl` and `httpUrl` properties are mutually exclusive, " +
            "and one of them must be specified. This URL must be readable and listable from compute nodes. There are " +
            "three ways to get such a URL for a container in Azure storage: include a Shared Access Signature (SAS) " +
            "granting read and list permissions on the container, use a managed identity with read and list " +
            "permissions, or set the ACL for the container to allow public access."
    )
    Property<String> storageContainerUrl;

    @Schema(
        title = "The URL of the file to download.",
        description = "The `autoStorageContainerName`, `storageContainerUrl` and `httpUrl` properties are mutually exclusive, " +
            "and one of them must be specified. If the URL points to Azure Blob Storage, it must be readable from " +
            "compute nodes. There are three ways to get such a URL for a blob in Azure storage: include a Shared Access " +
            "Signature (SAS) granting read permissions on the blob, use a managed identity with read permission, or " +
            "set the ACL for the blob or its container to allow public access."
    )
    Property<String> httpUrl;

    @Schema(
        title = "The blob prefix to use when downloading blobs from the Azure Storage container.",
        description = "Only the blobs whose names begin with the specified prefix will be downloaded. The property is " +
            "valid only when `autoStorageContainerName` or `storageContainerUrl` is used. This prefix can be a partial " +
            "file name or a subdirectory. If a prefix is not specified, all the files in the container will be downloaded."
    )
    Property<String> blobPrefix;

    @Schema(
        title = "The location on the Compute Node to which to download the file(s), relative to the Task's working directory.",
        description = "If the `httpUrl` property is specified, the `filePath` is required and describes the path which the " +
            "file will be downloaded to, including the file name. Otherwise, if the `autoStorageContainerName` or " +
            "`storageContainerUrl` property is specified, `filePath` is optional and is the directory to download the files " +
            "to. In the case where `filePath` is used as a directory, any directory structure already associated with the " +
            "input data will be retained in full and appended to the specified `filePath` directory. The specified " +
            "relative path cannot break out of the Task's working directory (for example by using `..`)."
    )
    Property<String> filePath;

    @Schema(
        title = "The file permission mode attribute in octal format.",
        description = "This property applies only to files being downloaded to Linux Compute Nodes. It will be ignored " +
            "if it is specified for a `resourceFile` which will be downloaded to a Windows Compute Node. If this property " +
            "is not specified for a Linux Compute Node, then a default value of `0770` is applied to the file."
    )
    Property<String> fileMode;

    @Schema(
        title = "The reference to the user assigned identity to use to access Azure Blob Storage specified by `storageContainerUrl` or `httpUrl`."
    )
    @PluginProperty(dynamic = true)
    ComputeNodeIdentityReference identityReference;

    public com.microsoft.azure.batch.protocol.models.ResourceFile to(RunContext runContext) throws IllegalVariableEvaluationException {
        return new com.microsoft.azure.batch.protocol.models.ResourceFile()
            .withAutoStorageContainerName(runContext.render(this.autoStorageContainerName).as(String.class).orElse(null))
            .withStorageContainerUrl(runContext.render(this.storageContainerUrl).as(String.class).orElse(null))
            .withHttpUrl(runContext.render(this.httpUrl).as(String.class).orElse(null))
            .withBlobPrefix(runContext.render(this.blobPrefix).as(String.class).orElse(null))
            .withFilePath(runContext.render(this.filePath).as(String.class).orElse(null))
            .withFileMode(runContext.render(this.fileMode).as(String.class).orElse(null))
            .withIdentityReference(identityReference == null ? null : identityReference.to(runContext));
    }
}
