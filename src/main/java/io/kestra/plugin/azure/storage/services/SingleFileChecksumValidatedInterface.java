package io.kestra.plugin.azure.storage.services;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;

public interface SingleFileChecksumValidatedInterface extends ChecksumValidatedInterface {
    @Schema(
        title = "Expected checksum",
        description = """
            User-supplied expected checksum for the downloaded file. Accepts a lowercase
            or uppercase hex digest, or the equivalent base64 encoding (Azure's native
            Content-MD5 format). Takes precedence over validateChecksum."""
    )
    @PluginProperty(group = "advanced")
    Property<String> getExpectedChecksum();

    @Schema(
        title = "Checksum algorithm",
        description = """
            Algorithm used to compute and compare the checksum. Defaults to MD5.
            Note: validateChecksum (server-side) only supports MD5, since that is what
            Azure stores. SHA-256 requires expectedChecksum."""
    )
    @PluginProperty(group = "advanced")
    Property<ChecksumValidator.Algorithm> getChecksumAlgorithm();
}
