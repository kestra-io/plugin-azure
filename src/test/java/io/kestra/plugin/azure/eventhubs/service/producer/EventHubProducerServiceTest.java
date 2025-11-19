package io.kestra.plugin.azure.eventhubs.service.producer;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventDataBatch;
import com.azure.messaging.eventhubs.EventHubProducerAsyncClient;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.azure.eventhubs.client.EventHubClientFactory;
import io.kestra.plugin.azure.eventhubs.config.EventHubConsumerConfig;
import io.kestra.plugin.azure.eventhubs.model.EventDataObject;
import io.kestra.plugin.azure.eventhubs.serdes.StringSerde;
import io.kestra.plugin.azure.eventhubs.service.EventDataObjectConverter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@ExtendWith(MockitoExtension.class)
class EventHubProducerServiceTest {

    private static final Logger LOG = LoggerFactory.getLogger(EventHubProducerServiceTest.class);

    @Captor
    ArgumentCaptor<EventDataBatch> eventDataBatchArgumentCaptor;

    @Captor
    ArgumentCaptor<EventData> eventDataArgumentCaptor;

    @Mock
    private EventHubProducerAsyncClient producer;
    @Mock
    private EventHubClientFactory factory;
    private EventDataObjectConverter converter;

    @BeforeEach
    public void beforeEach() throws IllegalVariableEvaluationException {
        converter = new EventDataObjectConverter(new StringSerde());
        Mockito.when(factory.createAsyncProducerClient(Mockito.any())).thenReturn(producer);
        Mockito.when(producer.send(eventDataBatchArgumentCaptor.capture())).thenReturn(Mono.empty());
    }

    @Test
    void shouldSendExceptionGivenTooLargeEvent() throws Exception {
        // GIVEN
        int maxRecordPerBatch = 1;
        EventHubProducerService service = createNewSenderService(0);
        List<EventDataObject> events = List.of(new EventDataObject("message"));
        byte[] data = getDataAsBytesFor(events);

        // WHEN
        try (BufferedReader stream = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(data)))) {
            ProducerContext options = new ProducerContext(
                null, null, maxRecordPerBatch, LOG
            );
            // THEN
            Assertions.assertThrows(IllegalArgumentException.class, () -> service.sendEvents(stream, options));
        }
    }

    @Test
    void shouldSendGivenStreamOfOneEvent() throws Exception {
        // GIVEN
        int maxRecordPerBatch = 1;
        EventHubProducerService service = createNewSenderService(1024);
        List<EventDataObject> events = List.of(new EventDataObject("message"));
        byte[] data = getDataAsBytesFor(events);

        // WHEN
        try (BufferedReader stream = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(data)))) {

            ProducerContext options = new ProducerContext(
                null, null, maxRecordPerBatch, LOG
            );
            EventHubProducerService.Result result = service.sendEvents(stream, options);
            // THEN
            Mockito.verify(producer, Mockito.times(1)).send(eventDataBatchArgumentCaptor.capture());
            Assertions.assertEquals(new EventHubProducerService.Result(1, 1), result);
        }
    }

    @Test
    void shouldSendGivenStreamOfMultipleEvents() throws Exception {
        // GIVEN
        int maxRecordPerBatch = 2;
        int expectedTotalNumRecords = 11;
        int expectedTotalNumBatches = (expectedTotalNumRecords / 2) + expectedTotalNumRecords % maxRecordPerBatch;

        EventHubProducerService service = createNewSenderService(1024);
        List<EventDataObject> events = IntStream
            .range(0, expectedTotalNumRecords)
            .mapToObj(i -> new EventDataObject("message-" + i))
            .collect(Collectors.toList());

        byte[] data = getDataAsBytesFor(events);

        // WHEN
        try (BufferedReader stream = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(data)))) {

            ProducerContext options = new ProducerContext(
                null, null, maxRecordPerBatch, LOG
            );
            EventHubProducerService.Result result = service.sendEvents(stream, options);

            // THEN
            Mockito.verify(producer, Mockito.times(expectedTotalNumBatches)).send(eventDataBatchArgumentCaptor.capture());
            Assertions.assertEquals(
                events,
                converter.convertFromEventData(eventDataArgumentCaptor.getAllValues().stream().distinct().collect(Collectors.toList()))
            );
            Assertions.assertEquals(new EventHubProducerService.Result(expectedTotalNumRecords, expectedTotalNumBatches), result);
        }
    }

    private static byte[] getDataAsBytesFor(List<EventDataObject> entities) throws IOException {
        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        try (os) {
            for (EventDataObject entity : entities) {
                FileSerde.write(os, entity);
            }
        }
        return os.toByteArray();
    }

    private EventHubProducerService createNewSenderService(int maxBatchSize) {

        return new EventHubProducerService(
            factory,
            new EventHubConsumerConfig(null, null),
            converter,
            new EventDataBatchFactory() {
                @Override
                public Mono<EventDataBatch> createBatch(EventHubProducerAsyncClient client) {
                    final EventDataBatch batch = Mockito.mock(EventDataBatch.class);
                    final AtomicInteger counter = new AtomicInteger(0);
                    final AtomicInteger batchSize = new AtomicInteger(0);
                    Mockito.when(batch.tryAdd(eventDataArgumentCaptor.capture()))
                        .then((Answer<Boolean>) invocation -> {
                            EventData value = eventDataArgumentCaptor.getValue();
                            int currentBatchSize = batchSize.accumulateAndGet(value.getBody().length, Integer::sum);
                            if (currentBatchSize < maxBatchSize) {
                                counter.accumulateAndGet(1, Integer::sum);
                                return true;
                            }
                            return false;
                        });
                    if (maxBatchSize > 0) { // this necessary for not having UnnecessaryStubbingException
                        Mockito.when(batch.getCount()).then((Answer<Integer>) invocation -> counter.get());
                    }
                    return Mono.just(batch);
                }
            }
        );
    }
}