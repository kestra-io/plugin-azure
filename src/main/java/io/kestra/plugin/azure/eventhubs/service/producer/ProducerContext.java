package io.kestra.plugin.azure.eventhubs.service.producer;

import org.slf4j.Logger;

import java.util.Map;

/**
 * Options for publihsing events.
 *
 * @param bodyContentType   The default body content-type.
 * @param eventProperties   the default properties to add to events.
 * @param maxEventsPerBatch The maximum number of events per batch.
 */
public record ProducerContext(String bodyContentType,
                              Map<String, String> eventProperties,
                              Integer maxEventsPerBatch,
                              Logger     logger
) {
}
