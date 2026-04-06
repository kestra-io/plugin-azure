package io.kestra.plugin.azure.eventhubs;

import java.time.Duration;

import io.kestra.core.models.property.Property;

import io.swagger.v3.oas.annotations.media.Schema;
import io.kestra.core.models.annotations.PluginProperty;

public interface EventHubBatchConsumerInterface {

    @Schema(
        title = "The maximum number of events to consume per event hub partition per poll."
    )
    @PluginProperty(group = "execution")
    Property<Integer> getMaxBatchSizePerPartition();

    @Schema(
        title = "The max time duration to wait to receive a batch of events up to the `maxBatchSizePerPartition`."
    )
    @PluginProperty(group = "execution")
    Property<Duration> getMaxWaitTimePerPartition();

    @Schema(
        title = "The max time duration to wait to receive events from all partitions."
    )
    @PluginProperty(group = "execution")
    Property<Duration> getMaxDuration();
}
