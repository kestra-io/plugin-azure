package io.kestra.plugin.azure.storage.services;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;

public interface ChecksumValidatedInterface {
    @Schema(
        title = "Validate checksum against server's Content-MD5",
        description = """
            If true, compute the MD5 of the downloaded file and compare it to the
            Content-MD5 stored on the Azure object. Many block blobs uploaded as
            streams have no Content-MD5; see failOnMissingChecksum to control that case."""
    )
    @PluginProperty(group = "reliability")
    Property<Boolean> getValidateChecksum();

    @Schema(
        title = "Fail when the server has no Content-MD5",
        description = """
            Only applies when validateChecksum is true. If true, the task fails when
            the Azure object has no stored Content-MD5. If false (default), validation
            is skipped with a warning."""
    )
    @PluginProperty(group = "reliability")
    Property<Boolean> getFailOnMissingChecksum();
}
