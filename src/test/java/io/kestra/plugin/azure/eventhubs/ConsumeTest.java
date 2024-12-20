package io.kestra.plugin.azure.eventhubs;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.azure.eventhubs.serdes.Serdes;
import io.micronaut.context.annotation.Value;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

@KestraTest
class ConsumeTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.variables.globals.azure.eventhubs.connection-string}")
    protected String connectionString;

    @Value("${kestra.variables.globals.azure.eventhubs.checkpointstore.connection-string}")
    protected String checkPointStoreConnectionString;

    @Value("${kestra.variables.globals.azure.eventhubs.checkpointstore.container-name}")
    protected String checkPointStoreContainerName;

    @Value("${kestra.variables.globals.azure.eventhubs.eventhub-name}")
    protected String eventHubName;

    @Disabled
    @Test
    void testConsumeTask() throws Exception {
        // Given
        RunContext runContext = runContextFactory.of();

        Consume task = Consume.builder()
            .bodyDeserializer(Property.of(Serdes.STRING))
            .eventHubName(Property.of(eventHubName))
            .connectionString(Property.of(connectionString))
            .checkpointStoreProperties(Property.of(Map.of(
                    "connectionString", checkPointStoreConnectionString,
                    "containerName", checkPointStoreContainerName
                )
            ))
            .consumerGroup(Property.of("$Default"))
            .maxBatchSizePerPartition(Property.of(10))
            .maxWaitTimePerPartition(Property.of(Duration.ofSeconds(5)))
            .maxDuration(Property.of(Duration.ofSeconds(10)))
            .build();

        // When
        produceEvents();
        Consume.Output runOutput = task.run(runContext);

        // Then
        Assertions.assertEquals(2, runOutput.getEventsCount());
    }

    private void produceEvents() throws Exception {
        Produce task = Produce.builder()
            .id(ConsumeTest.class.getSimpleName())
            .type(Produce.class.getName())
            .bodySerializer(Property.of(Serdes.STRING))
            .eventHubName(Property.of(eventHubName))
            .connectionString(Property.of(connectionString))
            .from(List.of(
                ImmutableMap.builder()
                    .put("body", "event-1")
                    .build(),
                ImmutableMap.builder()
                    .put("body", "event-2")
                    .build()
            ))
            .build();
        task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));
    }
}
