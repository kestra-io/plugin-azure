package io.kestra.plugin.azure.storage.blob.interfaces;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;

public interface AbstractBlobStorageInterface {
    @Schema(
        title = "Connection string of the storage account."
    )
    @PluginProperty(dynamic = true)
    String getConnectionString();

    @Schema(
        title = "Shared Key account name for authenticating requests"
    )
    @PluginProperty(dynamic = true)
    String getSharedKeyAccountName();


    @Schema(
        title = "Shared Key access key for authenticating requests"
    )
    @PluginProperty(dynamic = true)
    String getSharedKeyAccountAccessKey();

    @Schema(
        title = "The SAS token to use for authenticating requests.",
        description = "This string should only be the query parameters (with or without a leading '?') and not a full url."
    )
    @PluginProperty(dynamic = true)
    String getSasToken();
}
