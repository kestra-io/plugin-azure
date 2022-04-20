package io.kestra.plugin.azure.storage.blob.models;

import com.azure.storage.blob.models.BlobImmutabilityPolicyMode;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Value;

import java.time.ZonedDateTime;

@Builder
@Value
public class BlobImmutabilityPolicy {
    @Schema(
        title = "The time when the immutability policy expires."
    )
    @PluginProperty(dynamic = false)
    ZonedDateTime expiryTime;

    @Schema(
        title = "The immutability policy mode."
    )
    @PluginProperty(dynamic = false)
    BlobImmutabilityPolicyMode policyMode;

    public com.azure.storage.blob.models.BlobImmutabilityPolicy to(RunContext runContext) {
        com.azure.storage.blob.models.BlobImmutabilityPolicy item = new com.azure.storage.blob.models.BlobImmutabilityPolicy();

        item.setExpiryTime(this.expiryTime.toOffsetDateTime());
        item.setPolicyMode(this.policyMode);

        return item;
    }
}
