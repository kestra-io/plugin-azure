package io.kestra.plugin.azure.eventhubs.config;

import com.azure.messaging.eventhubs.models.EventPosition;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.eventhubs.EventHubConsumerInterface;
import io.kestra.plugin.azure.eventhubs.service.consumer.EventPositionStrategy;
import io.kestra.plugin.azure.eventhubs.service.consumer.StartingPosition;

/**
 * Configuration that uses the {@link RunContext} to render configuration.
 */
public final class EventHubConsumerConfig extends EventHubClientConfig<EventHubConsumerInterface> {

    /**
     * Creates a new {@link EventHubConsumerConfig} instance.
     *
     * @param runContext The context. Cannot be null.
     * @param plugin     The plugin. Cannot be null.
     */
    public EventHubConsumerConfig(final RunContext runContext,
                                  final EventHubConsumerInterface plugin) {
        super(runContext, plugin);
    }

    public String consumerGroup() throws IllegalVariableEvaluationException {
        return runContext.render(plugin.getConsumerGroup());
    }

    public EventPosition partitionStartingPosition() {
        StartingPosition partitionStartingPosition = plugin.getPartitionStartingPosition();
        return switch (partitionStartingPosition) {
            case EARLIEST -> new EventPositionStrategy.Earliest().get();
            case LATEST -> new EventPositionStrategy.Latest().get();
            case INSTANT -> new EventPositionStrategy.EnqueuedTime(plugin.getEnqueueTime()).get();
        };
    }
}
