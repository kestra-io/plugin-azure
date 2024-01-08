package io.kestra.plugin.azure.eventhubs;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.plugin.azure.eventhubs.serdes.Serdes;
import io.kestra.plugin.azure.eventhubs.service.consumer.StartingPosition;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Duration;
import java.util.Map;

/**
 * Base class for implementing tasks that consume events into EventHubs.
 * This class provides all required and optional parameters.
 */
public interface EventHubConsumerInterface extends EventHubClientInterface {

    // TASK'S PARAMETERS
    @Schema(
        title = "The Deserializer to be used for serializing the event value."
    )
    @PluginProperty
    Serdes getBodyDeserializer();

    @Schema(
        title = "The config properties to be passed to the Deserializer.",
        description = "Configs in key/value pairs."
    )
    @PluginProperty
    Map<String, Object> getBodyDeserializerProperties();

    @Schema(
        title = "The consumer group."
    )
    @PluginProperty
    String getConsumerGroup();

    @Schema(
        title = "The starting position."
    )
    @PluginProperty
    StartingPosition getPartitionStartingPosition();

    @Schema(
        title = "The ISO Datetime to be used when `PartitionStartingPosition` is configured to `INSTANT`.",
        description = "Configs in key/value pairs."
    )
    @PluginProperty
    String getEnqueueTime();

    @Schema(
        title = "The maximum number of events to consume per event hub partition per poll."
    )
    @PluginProperty
    Integer getMaxBatchSizePerPartition();

    @Schema(
        title = "The max time duration to wait to receive a batch of events up to the `maxBatchSizePerPartition`."
    )
    @PluginProperty
    Duration getMaxWaitTimePerPartition();

    @Schema(
        title = "The max time duration to wait to receive events from all partitions."
    )
    @PluginProperty
    Duration getMaxDuration();

    @Schema(
        title = "The config properties to be used for configuring the BlobCheckpointStore.",
        description = "Azure Event Hubs Checkpoint Store can be used for storing checkpoints while processing events from Azure Event Hubs."
    )
    @PluginProperty
    Map<String, String> getCheckpointStoreProperties();

}
