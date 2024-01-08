package io.kestra.plugin.azure.eventhubs.service.producer;

import com.azure.messaging.eventhubs.EventDataBatch;
import com.azure.messaging.eventhubs.EventHubProducerAsyncClient;
import com.azure.messaging.eventhubs.models.CreateBatchOptions;
import reactor.core.publisher.Mono;

/**
 * Service interface for constructing new {@link EventDataBatch} objects.
 */
public interface EventDataBatchFactory {

    /**
     * Factry method to create a new batch.
     *
     * @param client The {@link EventHubProducerAsyncClient}.
     * @return a new {@link EventDataBatch}.
     */
    Mono<EventDataBatch> createBatch(EventHubProducerAsyncClient client);

    /**
     * Default factory for creating bath from the given options and client.
     *
     * @param options The options to be used for creating new batch.
     */
    record Default(CreateBatchOptions options) implements EventDataBatchFactory {

        /**
         * {@inheritDoc}
         **/
        @Override
        public Mono<EventDataBatch> createBatch(EventHubProducerAsyncClient client) {
            return client.createBatch(options);
        }
    }
}
