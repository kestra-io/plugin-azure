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
    title = "Delete an entity in an Azure Storage Table."
)
public class Delete extends AbstractTableStorage implements RunnableTask<VoidOutput> {
    @Schema(
        title = "The partition key of the entity."
    )
    @NotNull
    private Property<String> partitionKey;

    @Schema(
        title = "The row key of the entity."
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
