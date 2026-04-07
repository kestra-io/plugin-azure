package io.kestra.plugin.azure.eventhubs;

import java.util.Map;

import io.kestra.core.models.property.Property;
import io.kestra.plugin.azure.eventhubs.serdes.Serdes;
import io.kestra.plugin.azure.eventhubs.service.consumer.StartingPosition;

import io.swagger.v3.oas.annotations.media.Schema;
import io.kestra.core.models.annotations.PluginProperty;

/**
 * Base class for implementing tasks that consume events into EventHubs.
 * This class provides all required and optional parameters.
 */
public interface EventHubConsumerInterface extends EventHubClientInterface {

    // TASK'S PARAMETERS
    @Schema(
        title = "The Deserializer to be used for serializing the event value."
    )
    @PluginProperty(group = "advanced")
    Property<Serdes> getBodyDeserializer();

    @Schema(
        title = "The config properties to be passed to the Deserializer.",
        description = "Configs in key/value pairs."
    )
    @PluginProperty(group = "advanced")
    Property<Map<String, Object>> getBodyDeserializerProperties();

    @Schema(
        title = "The consumer group."
    )
    @PluginProperty(group = "advanced")
    Property<String> getConsumerGroup();

    @Schema(
        title = "The starting position."
    )
    @PluginProperty(group = "advanced")
    Property<StartingPosition> getPartitionStartingPosition();

    @Schema(
        title = "The ISO Datetime to be used when `PartitionStartingPosition` is configured to `INSTANT`.",
        description = "Configs in key/value pairs."
    )
    @PluginProperty(group = "advanced")
    Property<String> getEnqueueTime();

    @Schema(
        title = "The config properties to be used for configuring the BlobCheckpointStore.",
        description = "Azure Event Hubs Checkpoint Store can be used for storing checkpoints while processing events from Azure Event Hubs."
    )
    @PluginProperty(group = "advanced")
    Property<Map<String, String>> getCheckpointStoreProperties();

}
