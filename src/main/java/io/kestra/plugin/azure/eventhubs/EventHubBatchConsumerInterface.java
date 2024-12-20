package io.kestra.plugin.azure.eventhubs;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Duration;

public interface EventHubBatchConsumerInterface {

    @Schema(
        title = "The maximum number of events to consume per event hub partition per poll."
    )
    Property<Integer> getMaxBatchSizePerPartition();

    @Schema(
        title = "The max time duration to wait to receive a batch of events up to the `maxBatchSizePerPartition`."
    )
    Property<Duration> getMaxWaitTimePerPartition();

    @Schema(
        title = "The max time duration to wait to receive events from all partitions."
    )
    Property<Duration> getMaxDuration();
}
