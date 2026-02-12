package io.kestra.plugin.azure.storage.table;

import com.azure.data.tables.TableClient;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.storage.table.abstracts.AbstractTableStorage;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import jakarta.validation.constraints.NotNull;

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
                id: azure_storage_table_delete
                namespace: company.team

                tasks:
                  - id: delete
                    type: io.kestra.plugin.azure.storage.table.Delete
                    endpoint: "https://yourstorageaccount.table.core.windows.net"
                    connectionString: "DefaultEndpointsProtocol=...=="
                    table: "table_name"
                    partitionKey: "color"
                    rowKey: "green"
                """
        )
    }
)
@Schema(
    title = "Delete one Table entity",
    description = "Removes an entity by partitionKey and rowKey. Uses unconditional delete (no ETag check); fails if the entity is missing."
)
public class Delete extends AbstractTableStorage implements RunnableTask<VoidOutput> {
    @Schema(
        title = "Partition key to delete",
        description = "Partition key of the entity to delete."
    )
    @NotNull
    private Property<String> partitionKey;

    @Schema(
        title = "Row key to delete",
        description = "Row key of the entity to delete."
    )
    @NotNull
    private Property<String> rowKey;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        TableClient tableClient = this.tableClient(runContext);

        tableClient.deleteEntity(
            runContext.render(this.partitionKey).as(String.class).orElseThrow(),
            runContext.render(this.rowKey).as(String.class).orElseThrow()
        );

        return null;
    }
}
