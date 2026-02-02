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
        title = "Auto storage container name",
        description = "Mutually exclusive with storageContainerUrl and httpUrl; one of the three is required"
    )
    Property<String> autoStorageContainerName;

    @Schema(
        title = "Blob container URL",
        description = "SAS/identity/public URL to a container; mutually exclusive with autoStorageContainerName and httpUrl; must allow read+list from compute nodes"
    )
    Property<String> storageContainerUrl;

    @Schema(
        title = "HTTP/HTTPS file URL",
        description = "Direct file URL; mutually exclusive with storageContainerUrl and autoStorageContainerName; must be readable from compute nodes"
    )
    Property<String> httpUrl;

    @Schema(
        title = "Blob prefix filter",
        description = "Filters blobs when using autoStorageContainerName or storageContainerUrl; downloads only names starting with this prefix"
    )
    Property<String> blobPrefix;

    @Schema(
        title = "Target path on node",
        description = "Destination relative to task working dir; required for httpUrl (includes filename); optional dir for container downloads"
    )
    Property<String> filePath;

    @Schema(
        title = "File mode (octal)",
        description = "Linux-only permission bits; defaults to 0770 on Linux; ignored on Windows nodes"
    )
    Property<String> fileMode;

    @Schema(
        title = "User-assigned identity reference",
        description = "Identity used to access storage when using storageContainerUrl or httpUrl"
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
