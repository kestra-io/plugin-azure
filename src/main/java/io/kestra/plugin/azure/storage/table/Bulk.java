package io.kestra.plugin.azure.storage.table;

import com.azure.data.tables.TableClient;
import com.azure.data.tables.models.TableTransactionAction;
import com.azure.data.tables.models.TableTransactionActionType;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.azure.storage.table.abstracts.AbstractTableStorage;
import io.kestra.plugin.azure.storage.table.models.Entity;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.List;
import java.util.Map;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            code = {
                "endpoint: \"https://yourblob.blob.core.windows.net\"",
                "connectionString: \"DefaultEndpointsProtocol=...==\"",
                "table: \"mydata\"",
                "from:",
                "  - partitionKey: \"color\"",
                "    rowKey: \"green\"",
                "    type: \"UPSERT_MERGE\"",
                "    properties:",
                "      \"code\": \"00FF00\""
            }
        )
    }
)
@Schema(
    title = "Inserts or updates entities into the Azure Storage Table. Make sure to pass either a list of entities or a file with a list of entities."
)
public class Bulk extends AbstractTableStorage implements RunnableTask<Bulk.Output> {
    @Schema(
        title = "Source of a message.",
        description = "Can be an internal storage URI or a list of maps " +
            "in the format `partitionKey`, `rowKey`, `type`, `properties`, as shown in the example."
    )
    @NotNull
    @PluginProperty(dynamic = true)
    private Object from;

    @Schema(
        title = "The default operation type to be applied to the entity."
    )
    @NotNull
    @PluginProperty(dynamic = true)
    @Builder.Default
    private TableTransactionActionType defaultType = TableTransactionActionType.UPSERT_REPLACE;

    @SuppressWarnings("unchecked")
    @Override
    public Bulk.Output run(RunContext runContext) throws Exception {
        TableClient tableClient = this.tableClient(runContext);
        BufferedReader inputStream = null;

        try {
            Flowable<Object> flowable;

            if (this.from instanceof String) {
                URI from = new URI(runContext.render((String) this.from));
                inputStream = new BufferedReader(new InputStreamReader(runContext.uriToInputStream(from)));
                flowable = Flowable.create(FileSerde.reader(inputStream), BackpressureStrategy.BUFFER);
            } else if (this.from instanceof List) {
                flowable = Flowable.fromArray(((List<Entity>) this.from).toArray());
            } else {
                flowable = Flowable.fromArray(this.createEntity(this.from).to());
            }

            Integer count = flowable
                .map(row -> {
                    Entity entity = this.createEntity(row);

                    return new TableTransactionAction(entity.getType() != null ? entity.getType() : defaultType, entity.to());
                })
                .buffer(100, 100)
                .map(o -> {
                    tableClient.submitTransaction(o);

                    return o.size();
                })
                .reduce(Integer::sum)
                .blockingGet();

            runContext.metric(Counter.of("records", count, "table", tableClient.getTableName()));

            return Output.builder()
                .count(count)
                .build();
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }

    }

    @SuppressWarnings("unchecked")
    private Entity createEntity(Object row) {
        if (row instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) row;
            return Entity.builder()
                .partitionKey((String) map.get("partitionKey"))
                .rowKey((String) map.get("rowKey"))
                .properties((Map<String, Object>) map.get("properties"))
                .type(map.containsKey("type") ? TableTransactionActionType.valueOf((String) map.get("type")) : null)
                .build();
        } else {
            throw new IllegalArgumentException("Invalid type '" + row.getClass() + "' on '" + row + "'");
        }
    }

    @SuperBuilder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Number of entities created."
        )
        private final Integer count;
    }
}
