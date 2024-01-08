package io.kestra.plugin.azure.eventhubs;

import com.azure.messaging.eventhubs.EventDataBatch;
import com.azure.messaging.eventhubs.EventHubProducerAsyncClient;
import com.azure.messaging.eventhubs.models.CreateBatchOptions;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.azure.eventhubs.client.EventHubClientFactory;
import io.kestra.plugin.azure.eventhubs.config.EventHubConsumerConfig;
import io.kestra.plugin.azure.eventhubs.serdes.StringSerde;
import io.kestra.plugin.azure.eventhubs.service.EventDataObjectConverter;
import io.kestra.plugin.azure.eventhubs.service.producer.EventDataBatchFactory;
import io.kestra.plugin.azure.eventhubs.service.producer.EventHubProducerService;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;

import java.util.Map;

@MicronautTest
@ExtendWith(MockitoExtension.class)
class ProduceTest {

    static final EventHubConsumerConfig EMPTY_CONFIG = new EventHubConsumerConfig(null, null);

    @Inject
    private RunContextFactory runContextFactory;

    @Mock
    private EventHubProducerAsyncClient client;
    @Mock
    private EventHubClientFactory factory;
    private EventDataObjectConverter converter;

    @BeforeEach
    public void beforeEach() throws IllegalVariableEvaluationException {
        converter = new EventDataObjectConverter(new StringSerde());
        Mockito.when(factory.createAsyncProducerClient(Mockito.any())).thenReturn(client);
        Mockito.when(client.send(Mockito.any(EventDataBatch.class))).thenReturn(Mono.empty());
    }

    @Test
    void testGivenFromMap() throws Exception {
        // Given
        RunContext runContext = runContextFactory.of();

        Produce task = Produce.builder()
            .from(Map.of("body", "msg"))
            .eventHubName("test")
            .build();
        // create mocks
        EventDataBatch batch = Mockito.mock(EventDataBatch.class);
        Mockito.when(batch.tryAdd(Mockito.any())).thenReturn(true);
        Mockito.when(batch.getCount()).thenReturn(1);
        Mockito.when(client.createBatch(Mockito.any())).thenReturn(Mono.just(batch));

        EventHubProducerService service = new EventHubProducerService(
            factory,
            EMPTY_CONFIG,
            converter,
            new EventDataBatchFactory.Default(new CreateBatchOptions())) {
        };

        // When
        Produce.Output runOutput = task.run(runContext, service);

        // Then
        Assertions.assertEquals(1, runOutput.getEventsCount());
    }

    @Test
    void testGivenFromList() throws Exception {
        // Given
        RunContext runContext = runContextFactory.of();

        Produce task = Produce.builder()
            .from(Map.of("body", "msg"))
            .eventHubName("test")
            .build();
        // create mocks
        EventDataBatch batch = Mockito.mock(EventDataBatch.class);
        Mockito.when(batch.tryAdd(Mockito.any())).thenReturn(true);
        Mockito.when(batch.getCount()).thenReturn(2);
        Mockito.when(client.createBatch(Mockito.any())).thenReturn(Mono.just(batch));

        EventHubProducerService service = new EventHubProducerService(
            factory,
            EMPTY_CONFIG,
            converter,
            new EventDataBatchFactory.Default(new CreateBatchOptions())) {
        };

        // When
        Produce.Output runOutput = task.run(runContext, service);

        // Then
        Assertions.assertEquals(2, runOutput.getEventsCount());
    }
}