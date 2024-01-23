package io.kestra.plugin.azure.storage.table;

import com.azure.data.tables.TableClient;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
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
                "partitionKey: \"color\"",
                "rowKey: \"green\""
            }
        )
    }
)
@Schema(
    title = "Delete an entity on the Azure Storage Table."
)
public class Delete extends AbstractTableStorage implements RunnableTask<VoidOutput> {
    @Schema(
        title = "The partition key of the entity."
    )
    @NotNull
    @PluginProperty(dynamic = true)
    private String partitionKey;

    @Schema(
        title = "The row key of the entity."
    )
    @PluginProperty(dynamic = true)
    private String rowKey;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        TableClient tableClient = this.tableClient(runContext);

        tableClient.deleteEntity(
            runContext.render(this.partitionKey),
            runContext.render(this.rowKey)
        );

        return null;
    }
}
