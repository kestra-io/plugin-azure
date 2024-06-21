package io.kestra.plugin.azure.storage.table;

import com.azure.data.tables.TableClient;
import com.azure.data.tables.models.ListEntitiesOptions;
import com.azure.data.tables.models.TableEntity;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.azure.storage.table.abstracts.AbstractTableStorage;
import io.kestra.plugin.azure.storage.table.models.Entity;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            code = {
                "endpoint: \"https://yourstorageaccount.table.core.windows.net\"",
                "connectionString: \"DefaultEndpointsProtocol=...==\"",
                "table: \"table_name\"",
            }
        )
    }
)
@Schema(
    title = "Lists entities from the Azure Storage Table using the parameters in the provided options.",
    description = "If the `filter` parameter in the options is set, only entities matching the filter will be returned.\n" +
        "If the `select` parameter is set, only the properties included in the select parameter will be returned for each entity.\n" +
        "If the `top` parameter is set, the maximum number of returned entities per page will be limited to that value."
)
public class List extends AbstractTableStorage implements RunnableTask<List.Output> {
    @Schema(
        title = "Returns only tables or entities that satisfy the specified filter.",
        description = "You can specify the filter using [Filter Strings](https://docs.microsoft.com/en-us/visualstudio/azure/vs-azure-tools-table-designer-construct-filter-strings?view=vs-2022)."
    )
    @PluginProperty(dynamic = true)
    private String filter;

    @Schema(
        title = "The desired properties of an entity from the Azure Storage Table."
    )
    @PluginProperty(dynamic = true)
    private java.util.List<String> select;

    @Schema(
        title = "List the top `n` tables or entities from the Azure Storage Table."
    )
    @PluginProperty(dynamic = true)
    private Integer top;

    @Override
    public List.Output run(RunContext runContext) throws Exception {
        Integer count = 0;

        TableClient tableClient = this.tableClient(runContext);

        ListEntitiesOptions options = new ListEntitiesOptions();

        if (this.filter != null) {
            options.setFilter(runContext.render(this.filter));
        }

        if (this.select != null) {
            options.setSelect(runContext.render(this.select));
        }

        if (this.top != null) {
            options.setTop(this.top);
        }

        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        try (BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(tempFile))) {
            for (TableEntity entity : tableClient.listEntities(options, null, null)) {
                FileSerde.write(output, Entity.to(entity));
                count++;
            }
        }

        runContext.metric(Counter.of("records", count, "table", tableClient.getTableName()));

        return Output.builder()
            .count(count)
            .uri(runContext.storage().putFile(tempFile))
            .build();
    }

    @SuperBuilder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Number of listed entities."
        )
        private final Integer count;

        @Schema(
            title = "URI of the Kestra internal storage file containing the output."
        )
        private URI uri;
    }
}
