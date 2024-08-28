package io.kestra.plugin.azure.eventhubs;

import com.azure.messaging.eventhubs.models.CreateBatchOptions;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.eventhubs.client.EventHubClientFactory;
import io.kestra.plugin.azure.eventhubs.config.EventHubClientConfig;
import io.kestra.plugin.azure.eventhubs.internal.InputStreamProvider;
import io.kestra.plugin.azure.eventhubs.serdes.Serdes;
import io.kestra.plugin.azure.eventhubs.service.EventDataObjectConverter;
import io.kestra.plugin.azure.eventhubs.service.producer.EventDataBatchFactory;
import io.kestra.plugin.azure.eventhubs.service.producer.EventHubProducerService;
import io.kestra.plugin.azure.eventhubs.service.producer.ProducerContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * The {@link RunnableTask} can be used for producing batches of events to Azure Event Hubs.
 */
@Plugin(examples = {
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
})
@Schema(
    title = "Publish events to Azure Event Hubs."
)
@Slf4j
@SuperBuilder
@Getter
@NoArgsConstructor
public class Produce extends AbstractEventHubTask implements RunnableTask<Produce.Output> {

    // TASK'S METRICS
    private static final String METRIC_SENT_EVENTS_NAME = "total-sent-events";
    private static final String METRIC_SENT_BATCHES_NAME = "total-sent-batches";

    // TASK'S PARAMETERS
    @Schema(
        title = "The event properties",
        description = "The event properties which may be used for passing metadata associated with the event" +
            " body during Event Hubs operations."
    )
    @PluginProperty
    @Builder.Default
    private Map<String, String> eventProperties = Collections.emptyMap();

    @Schema(
        title = "The content of the message to be sent to EventHub",
        description = "Can be an internal storage URI, a map (i.e. a list of key-value pairs) or a list of maps. " +
            "The following keys are supported: `from`, `contentType`, `properties`.",
        anyOf = {String.class, List.class, Map.class}
    )
    @NotNull
    @PluginProperty(dynamic = true)
    private Object from;

    @Schema(
        title = "The hashing key to be provided for the batches of events.",
        description = "Events with the same `partitionKey` are hashed and sent to the same partition. The provided " +
            "`partitionKey` will be used for all the events sent by the `Produce` task."
    )
    @PluginProperty
    private String partitionKey;

    @Schema(
        title = "The maximum size for batches of events, in bytes."
    )
    @PluginProperty
    private Integer maxBatchSizeInBytes;

    @Schema(
        title = "The maximum number of events per batches."
    )
    @PluginProperty
    @Builder.Default
    private Integer maxEventsPerBatch = 1000;

    @Schema(
        title = "The MIME type describing the event data",
        description = "The MIME type describing the data contained in event body allowing consumers to make informed" +
            " decisions for inspecting and processing the event."
    )
    @PluginProperty
    private String bodyContentType;

    @Schema(
        title = "The Serializer to be used for serializing the event value."
    )
    @PluginProperty
    @Builder.Default
    private Serdes bodySerializer = Serdes.STRING;

    @Schema(
        title = "The config properties to be passed to the Serializer.",
        description = "Configs in key/value pairs."
    )
    @PluginProperty
    @Builder.Default
    private Map<String, Object> bodySerializerProperties = Collections.emptyMap();

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
            new EventDataObjectConverter(getBodySerializer().create(getBodySerializerProperties())),
            new EventDataBatchFactory.Default(getCreateBatchOptions())
        );

        return run(runContext, service);

    }

    // VisibleForTesting
    @SuppressWarnings("unchecked")
    Output run(final RunContext runContext, final EventHubProducerService service) throws Exception {

        final InputStreamProvider reader = new InputStreamProvider(runContext);

        InputStream is;
        if (this.getFrom() instanceof String uri) {
            is = reader.get(runContext.render(uri));
        } else if (this.getFrom() instanceof Map data) {
            is = reader.get(data);
        } else if (this.getFrom() instanceof List data) {
            is = reader.get(data);
        } else {
            throw new IllegalArgumentException(
                "Unsupported type for task-property `from`: " + this.getFrom().getClass().getSimpleName()
            );
        }
        return send(runContext, service, is);
    }

    private Output send(final RunContext runContext,
                        final EventHubProducerService service,
                        final InputStream is) throws IllegalVariableEvaluationException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
            // Sends
            ProducerContext options = new ProducerContext(
                getBodyContentType(),
                getEventProperties(),
                getMaxEventsPerBatch(),
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

    private CreateBatchOptions getCreateBatchOptions() {
        CreateBatchOptions options = new CreateBatchOptions();
        if (getMaxBatchSizeInBytes() != null) {
            options.setMaximumSizeInBytes(getMaxBatchSizeInBytes());
        }

        if (getPartitionKey() != null) {
            options.setPartitionKey(getPartitionKey());
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
