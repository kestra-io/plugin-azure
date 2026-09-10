package io.kestra.plugin.azure.ml;

import java.time.Duration;

import io.kestra.core.models.property.Property;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;

/**
 * Polling frequency shared by {@link StartComputeInstance} and {@link StopComputeInstance}: compute instances
 * typically transition state within a couple of minutes, much faster than a training job.
 */
@Builder
@Getter
public class ComputeCheckFrequency {
    @Schema(title = "Max wait duration", description = "Stop polling and fail after this duration; defaults to PT10M")
    @Builder.Default
    private Property<Duration> maxDuration = Property.ofValue(Duration.ofMinutes(10));

    @Schema(title = "Polling interval", description = "Delay between status checks; defaults to PT10S")
    @Builder.Default
    private Property<Duration> interval = Property.ofValue(Duration.ofSeconds(10));
}
