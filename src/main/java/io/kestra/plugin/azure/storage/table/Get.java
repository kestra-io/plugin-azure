package io.kestra.plugin.azure.storage.table;

import com.azure.data.tables.TableClient;
import com.azure.data.tables.models.TableEntity;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.storage.table.abstracts.AbstractTableStorage;
import io.kestra.plugin.azure.storage.table.models.Entity;
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
                id: azure_storage_table_get
                namespace: company.team

                tasks:
                  - id: get
                    type: io.kestra.plugin.azure.storage.table.Get
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
    title = "Get an entity from an Azure Table Storage table."
)
public class Get extends AbstractTableStorage implements RunnableTask<Get.Output> {
    @Schema(
        title = "The partition key of the entity."
    )
    @NotNull
    private Property<String> partitionKey;

    @Schema(
        title = "The row key of the entity."
    )
    private Property<String> rowKey;

    @Override
    public Get.Output run(RunContext runContext) throws Exception {
        TableClient tableClient = this.tableClient(runContext);

        TableEntity entity = tableClient.getEntity(
            runContext.render(this.partitionKey).as(String.class).orElseThrow(),
            runContext.render(this.rowKey).as(String.class).orElseThrow()
        );

        return Output.builder()
            .row(Entity.to(entity))
            .build();
    }

    @SuperBuilder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The entity retrieved from the table."
        )
        private final Entity row;
    }
}
