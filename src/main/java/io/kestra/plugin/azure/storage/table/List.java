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
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.Map;
import java.util.stream.Collectors;
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
                "data: \"{{ inputs.file }}\"",
                "name: \"myblob\""
            }
        )
    }
)
@Schema(
    title = "Upload a file to a Azure Blob Storage."
)
public class List extends AbstractTableStorage implements RunnableTask<List.Output> {
    @Schema(
        title = "Source of message send",
        description = "Can be an internal storage uri, a map or a list." +
            "with the following format: partitionKey, rowKey, properties"
    )
    @NotNull
    @PluginProperty(dynamic = true)
    private Object from;

    @Schema(
        title = "Returns only tables or entities that satisfy the specified filter.",
        description = "using [Filter Strings](https://docs.microsoft.com/en-us/visualstudio/azure/vs-azure-tools-table-designer-construct-filter-strings?view=vs-2022) "
    )
    @PluginProperty(dynamic = true)
    private String filter;

    @Schema(
        title = "Returns the desired properties of an entity from the set."
    )
    @PluginProperty(dynamic = true)
    private java.util.List<String> select;

    @Schema(
        title = "Returns only the top `n` tables or entities from the set."
    )
    @PluginProperty(dynamic = true)
    private Integer top;

    @Override
    public List.Output run(RunContext runContext) throws Exception {
        Integer count = 0;

        TableClient tableClient = this.tableClient(runContext);

        ArrayList<String> propertiesToSelect = new ArrayList<>();
        propertiesToSelect.add("Product");
        propertiesToSelect.add("Price");

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

        File tempFile = runContext.tempFile(".ion").toFile();
        try (BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(tempFile))) {
            for (TableEntity entity : tableClient.listEntities(options, null, null)) {
                FileSerde.write(output, Entity.to(entity));
                count++;
            }
        }

        runContext.metric(Counter.of("records", count, "table", tableClient.getTableName()));

        return Output.builder()
            .count(count)
            .uri(runContext.putTempFile(tempFile))
            .build();
    }

    @SuperBuilder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Number of entity"
        )
        private final Integer count;

        @Schema(
            title = "URI of a kestra internal storage file"
        )
        private URI uri;
    }
}
