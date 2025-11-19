package io.kestra.plugin.azure.eventhubs.service.consumer;
import org.slf4j.Logger;

import io.kestra.plugin.azure.eventhubs.service.EventDataObjectConverter;


import java.time.Duration;

/**
 * Context for consuming events from Azure Event Hubs.
 *
 * @param maxPollEvents
 * @param maxBatchPartitionWait
 * @param maxDuration
 * @param converter
 * @param logger
 */
public record ConsumerContext(int maxPollEvents,
                              Duration maxBatchPartitionWait,
                              Duration maxDuration,
                              EventDataObjectConverter converter,
                              Logger logger) {
}
