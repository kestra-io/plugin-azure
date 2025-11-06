package io.kestra.plugin.azure.storage.table;

import com.azure.data.tables.TableClient;
import com.azure.data.tables.models.TableTransactionAction;
import com.azure.data.tables.models.TableTransactionActionType;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Data;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.storage.table.abstracts.AbstractTableStorage;
import io.kestra.plugin.azure.storage.table.models.Entity;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.List;
import java.util.Map;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: azure_storage_table_bulk
                namespace: company.team

                tasks:
                  - id: bulk
                    type: io.kestra.plugin.azure.storage.table.Bulk
                    endpoint: "https://yourstorageaccount.blob.core.windows.net"
                    connectionString: "DefaultEndpointsProtocol=...=="
                    table: "table_name"
                    from:
                      - partitionKey: "color"
                        rowKey: "green"
                        type: "UPSERT_MERGE"
                        properties:
                          "code": "00FF00"
                """
        )
    },
    metrics = {
        @Metric(name = "records.count", type = Counter.TYPE, description = "The total number of entities processed in the bulk operation.")
    }
)
@Schema(
    title = "Insert or update entities in an Azure Storage Table.",
    description = "Make sure to pass either a list of entities or a file with a list of entities."
)
public class Bulk extends AbstractTableStorage implements RunnableTask<Bulk.Output>, Data.From {
    @Schema(
        title = Data.From.TITLE,
        description = Data.From.DESCRIPTION,
        anyOf = {String.class, List.class, Map.class}
    )
    @NotNull
    private Object from;

    @Schema(
        title = "The default operation type to be applied to the entity."
    )
    @NotNull
    @Builder.Default
    private Property<TableTransactionActionType> defaultType = Property.ofValue(TableTransactionActionType.UPSERT_REPLACE);

    @Override
    public Bulk.Output run(RunContext runContext) throws Exception {
        TableClient tableClient = this.tableClient(runContext);

        Integer count = Data.from(this.from)
            .read(runContext)
            .map(throwFunction(row -> {
                Entity entity = this.createEntity(runContext, row);

                return new TableTransactionAction(entity.getType() != null ? entity.getType() : runContext.render(defaultType).as(TableTransactionActionType.class).orElseThrow(), entity.to());
            }))
            .buffer(100, 100)
            .map(o -> {
                tableClient.submitTransaction(o);

                return o.size();
            })
            .reduce(Integer::sum)
            .block();

        runContext.metric(Counter.of("records.count", count, "table", tableClient.getTableName()));

        return Output.builder()
            .count(count)
            .build();
    }

    @SuppressWarnings("unchecked")
    private Entity createEntity(RunContext runContext, Object row) throws IllegalVariableEvaluationException {
        if (row instanceof Map<?, ?> rowMap) {
            Map<String, Object> map = runContext.render((Map<String, Object>) rowMap);
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
