package io.kestra.plugin.azure.eventhubs;

import com.azure.messaging.eventhubs.models.CreateBatchOptions;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Data;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.azure.eventhubs.client.EventHubClientFactory;
import io.kestra.plugin.azure.eventhubs.config.EventHubClientConfig;
import io.kestra.plugin.azure.eventhubs.serdes.Serdes;
import io.kestra.plugin.azure.eventhubs.service.EventDataObjectConverter;
import io.kestra.plugin.azure.eventhubs.service.producer.EventDataBatchFactory;
import io.kestra.plugin.azure.eventhubs.service.producer.EventHubProducerService;
import io.kestra.plugin.azure.eventhubs.service.producer.ProducerContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The {@link RunnableTask} can be used for producing batches of events to Azure Event Hubs.
 */
@Plugin(
    examples = {
        @Example(
            title = "Publish a file as events into Azure EventHubs.",
            full = true,
            code = """
                id: azure_eventhubs_send_events
                namespace: company.team

                inputs:
                  - id: file
                    type: FILE
                    description: a CSV file with columns id, username, tweet, and timestamp

                tasks:
                  - id: read_csv_file
                    type: io.kestra.plugin.serdes.csv.CsvToIon
                    from: "{{ inputs.file }}"

                  - id: transform_row_to_json
                    type: io.kestra.plugin.scripts.nashorn.FileTransform
                    from: "{{ outputs.read_csv_file.uri }}"
                    script: |
                      var result = {
                        "body": {
                          "username": row.username,
                          "tweet": row.tweet
                        }
                      };
                      row = result

                  - id: send_to_eventhub
                    type: io.kestra.plugin.azure.eventhubs.Produce
                    from: "{{ outputs.transform_row_to_json.uri }}"
                    eventHubName: my_eventhub
                    namespace: my_event_hub_namespace
                    connectionString: "{{ secret('EVENTHUBS_CONNECTION') }}"
                    maxBatchSizeInBytes: 4096
                    maxEventsPerBatch: 100
                    bodySerializer: "JSON"
                    bodyContentType: application/json
                    eventProperties:
                      source: kestra
                """
        )
    },
    metrics = {
        @Metric(name = "events.sent.count", type = Counter.TYPE, description = "The total number of events sent."),
        @Metric(name = "batches.sent.count", type = Counter.TYPE, description = "The total number of batches sent.")
    }
)
@Schema(
    title = "Publish events to Azure Event Hubs."
)
@Slf4j
@SuperBuilder
@Getter
@NoArgsConstructor
public class Produce extends AbstractEventHubTask implements RunnableTask<Produce.Output>, Data.From {

    // TASK'S METRICS
    private static final String METRIC_SENT_EVENTS_NAME = "events.sent.count";
    private static final String METRIC_SENT_BATCHES_NAME = "batches.sent.count";

    // TASK'S PARAMETERS
    @Schema(
        title = "The event properties",
        description = "The event properties which may be used for passing metadata associated with the event" +
            " body during Event Hubs operations."
    )
    @Builder.Default
    private Property<Map<String, String>> eventProperties = Property.ofValue(new HashMap<>());

    @Schema(
        title = Data.From.TITLE,
        description = Data.From.DESCRIPTION,
        anyOf = {String.class, List.class, Map.class}
    )
    @NotNull
    private Object from;

    @Schema(
        title = "The hashing key to be provided for the batches of events.",
        description = "Events with the same `partitionKey` are hashed and sent to the same partition. The provided " +
            "`partitionKey` will be used for all the events sent by the `Produce` task."
    )
    private Property<String> partitionKey;

    @Schema(
        title = "The maximum size for batches of events, in bytes."
    )
    private Property<Integer> maxBatchSizeInBytes;

    @Schema(
        title = "The maximum number of events per batches."
    )
    @Builder.Default
    private Property<Integer> maxEventsPerBatch = Property.ofValue(1000);

    @Schema(
        title = "The MIME type describing the event data",
        description = "The MIME type describing the data contained in event body allowing consumers to make informed" +
            " decisions for inspecting and processing the event."
    )
    private Property<String> bodyContentType;

    @Schema(
        title = "The Serializer to be used for serializing the event value."
    )
    @Builder.Default
    private Property<Serdes> bodySerializer = Property.ofValue(Serdes.STRING);

    @Schema(
        title = "The config properties to be passed to the Serializer.",
        description = "Configs in key/value pairs."
    )
    @Builder.Default
    private Property<Map<String, Object>> bodySerializerProperties = Property.ofValue(new HashMap<>());

    // SERVICES
    @Getter(AccessLevel.NONE)
    @Builder.Default
    private EventHubClientFactory clientFactory = new EventHubClientFactory();

    /**
     * {@inheritDoc}
     **/
    @Override
    public Output run(RunContext runContext) throws Exception {
        EventHubProducerService service = new EventHubProducerService(
            clientFactory,
            new EventHubClientConfig<>(runContext, this),
            new EventDataObjectConverter(runContext.render(getBodySerializer()).as(Serdes.class).orElseThrow()
                .create(runContext.render(getBodySerializerProperties()).asMap(String.class, Object.class))
            ),
            new EventDataBatchFactory.Default(getCreateBatchOptions(runContext))
        );

        return run(runContext, service);

    }

    // VisibleForTesting
    Output run(final RunContext runContext, final EventHubProducerService service) throws Exception {
        InputStream is = Data.from(this.from)
            .read(runContext)
            .collectList()
            .<ByteArrayInputStream>handle((items, sink) -> {
                try {
                    ByteArrayOutputStream os = new ByteArrayOutputStream();
                    for (Map<String, Object> item : items) {
                        FileSerde.write(os, item);
                    }
                    sink.next(new ByteArrayInputStream(os.toByteArray()));
                } catch (IOException e) {
                    sink.error(new RuntimeException(e));
                }
            })
            .block();

        return send(runContext, service, is);
    }

    private Output send(final RunContext runContext,
                        final EventHubProducerService service,
                        final InputStream is) throws IllegalVariableEvaluationException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
            // Sends
            ProducerContext options = new ProducerContext(
                runContext.render(getBodyContentType()).as(String.class).orElse(null),
                runContext.render(getEventProperties()).asMap(String.class, String.class),
                runContext.render(getMaxEventsPerBatch()).as(Integer.class).orElse(null),
                runContext.logger()
            );
            EventHubProducerService.Result result = service.sendEvents(
                reader,
                options
            );

            // metrics
            runContext.metric(Counter.of(METRIC_SENT_EVENTS_NAME, result.totalSentEvents()));
            runContext.metric(Counter.of(METRIC_SENT_BATCHES_NAME, result.totalSentBatches()));

            return new Output(
                result.totalSentEvents(),
                result.totalSentBatches()
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private CreateBatchOptions getCreateBatchOptions(RunContext runContext) throws IllegalVariableEvaluationException {
        CreateBatchOptions options = new CreateBatchOptions();
        if (getMaxBatchSizeInBytes() != null) {
            options.setMaximumSizeInBytes(runContext.render(getMaxBatchSizeInBytes()).as(Integer.class).orElseThrow());
        }

        if (getPartitionKey() != null) {
            options.setPartitionKey(runContext.render(getPartitionKey()).as(String.class).orElseThrow());
        }
        return options;
    }

    @AllArgsConstructor
    @Getter
    public static final class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Total number of events processed by the task."
        )
        private final Integer eventsCount;

        @Schema(
            title = "Total number of batches sent to an Azure EventHubs."
        )
        private final Integer sendBatchesCount;
    }
}
