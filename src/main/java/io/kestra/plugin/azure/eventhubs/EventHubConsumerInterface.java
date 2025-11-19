package io.kestra.plugin.azure.eventhubs;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
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
    Property<Serdes> getBodyDeserializer();

    @Schema(
        title = "The config properties to be passed to the Deserializer.",
        description = "Configs in key/value pairs."
    )
    Property<Map<String, Object>> getBodyDeserializerProperties();

    @Schema(
        title = "The consumer group."
    )
    Property<String> getConsumerGroup();

    @Schema(
        title = "The starting position."
    )
    Property<StartingPosition> getPartitionStartingPosition();

    @Schema(
        title = "The ISO Datetime to be used when `PartitionStartingPosition` is configured to `INSTANT`.",
        description = "Configs in key/value pairs."
    )
    Property<String> getEnqueueTime();

    @Schema(
        title = "The config properties to be used for configuring the BlobCheckpointStore.",
        description = "Azure Event Hubs Checkpoint Store can be used for storing checkpoints while processing events from Azure Event Hubs."
    )
    Property<Map<String, String>> getCheckpointStoreProperties();

}
