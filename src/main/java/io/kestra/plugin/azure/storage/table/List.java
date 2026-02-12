package io.kestra.plugin.azure.storage.table;

import com.azure.data.tables.TableClient;
import com.azure.data.tables.models.ListEntitiesOptions;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.azure.storage.table.abstracts.AbstractTableStorage;
import io.kestra.plugin.azure.storage.table.models.Entity;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.net.URI;

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
                id: azure_storage_table_list
                namespace: company.team

                tasks:
                  - id: list
                    type: io.kestra.plugin.azure.storage.table.List
                    endpoint: "https://yourstorageaccount.table.core.windows.net"
                    connectionString: "DefaultEndpointsProtocol=...=="
                    table: "table_name"
                """
        )
    },
    metrics = {
        @Metric(name = "records.count", type = Counter.TYPE, description = "The total number of entities listed.")
    }
)
@Schema(
    title = "List Azure Table entities",
    description = "Streams entities to Kestra storage and emits records.count. Supports server-side filter/select and page-size limits."
)
public class List extends AbstractTableStorage implements RunnableTask<List.Output> {
    @Schema(
        title = "OData filter expression",
        description = "Server-side [filter strings](https://docs.microsoft.com/en-us/visualstudio/azure/vs-azure-tools-table-designer-construct-filter-strings?view=vs-2022) using Azure Tables syntax."
    )
    private Property<String> filter;

    @Schema(
        title = "Properties to return per entity",
        description = "List of property names to return; empty returns all properties."
    )
    private Property<java.util.List<String>> select;

    @Schema(
        title = "Max entities per page",
        description = "Limits the number of entities per page; Azure may still paginate further."
    )
    private Property<Integer> top;

    @Schema(
        title = "The maximum number of entities to return",
        description = "Limits the number of entities returned by the list operation. If not specified, all matching entities will be returned."
    )
    @Builder.Default
    private Property<Integer> maxFiles = Property.ofValue(25);

    @Override
    public List.Output run(RunContext runContext) throws Exception {
        TableClient tableClient = this.tableClient(runContext);

        ListEntitiesOptions options = new ListEntitiesOptions();

        if (this.filter != null) {
            options.setFilter(runContext.render(this.filter).as(String.class).orElseThrow());
        }

        var renderedSelect = runContext.render(this.select).asList(String.class);
        if (!renderedSelect.isEmpty()) {
            options.setSelect(renderedSelect);
        }

        if (this.top != null) {
            options.setTop(runContext.render(this.top).as(Integer.class).orElseThrow());
        }

        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        try (var output = new BufferedWriter(new FileWriter(tempFile))) {
            var flux = Flux.fromIterable(tableClient.listEntities(options, null, null)).map(Entity::to);

            Integer rMaxFiles = runContext.render(this.maxFiles).as(Integer.class).orElse(25);
            flux = flux.take(rMaxFiles);

            Mono<Long> longMono = FileSerde.writeAll(output, flux);
            Long count = longMono.block();

            if (count >= rMaxFiles) {
                runContext.logger().warn(
                    "Listing was limited to {} entities by maxFiles property. "
                        + "Increase the maxFiles property if you need more entities.",
                    rMaxFiles
                );
            }

            runContext.metric(Counter.of("records.count", count, "table", tableClient.getTableName()));

            return Output.builder()
                .count(count)
                .uri(runContext.storage().putFile(tempFile))
                .build();
        }
    }

    @SuperBuilder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Total entities listed"
        )
        private final Long count;

        @Schema(
            title = "Result file storage URI",
            description = "kestra:// URI pointing to the ION file containing listed entities."
        )
        private URI uri;
    }
}
