package io.kestra.plugin.azure.horizondb.durable;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.azure.horizondb.AbstractHorizonDb;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "List pg_durable instances",
    description = "Queries `df.list_instances()`, with an optional status filter, and returns matching instances according to fetchType (FETCH or STORE)."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: horizondb_durable_list_instances
                namespace: company.team

                inputs:
                  - id: host
                    type: STRING

                tasks:
                  - id: list_running
                    type: io.kestra.plugin.azure.horizondb.durable.ListInstances
                    host: "{{ inputs.host }}"
                    port: 5432
                    database: mydb
                    username: "{{ secret('HORIZONDB_USERNAME') }}"
                    password: "{{ secret('HORIZONDB_PASSWORD') }}"
                    statusFilter: Running
                    fetchType: FETCH
                """
        )
    }
)
public class ListInstances extends AbstractHorizonDb<ListInstances.Output> implements RunnableTask<ListInstances.Output> {
    @Schema(
        title = "Status filter",
        description = "Optional status to filter instances by (e.g. Running, Completed, Failed, Cancelled). When empty, all instances are returned."
    )
    @PluginProperty(group = "main")
    protected Property<String> statusFilter;

    @Schema(
        title = "Result fetching mode",
        description = "FETCH returns all matching instances as a list, STORE streams them to internal storage (ION). NONE and FETCH_ONE are not meaningful here and are treated as FETCH / first row respectively."
    )
    @Builder.Default
    @PluginProperty(group = "main")
    protected Property<FetchType> fetchType = Property.ofValue(FetchType.FETCH);

    @Schema(
        title = "JDBC fetch size",
        description = "Number of rows fetched per database round trip; only used for STORE mode."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    protected Property<Integer> fetchSize = Property.ofValue(10000);

    @Override
    protected Output run(RunContext runContext, Connection connection) throws Exception {
        String rStatusFilter = runContext.render(statusFilter).as(String.class).orElse(null);
        FetchType rFetchType = runContext.render(fetchType).as(FetchType.class).orElse(FetchType.FETCH);
        Integer rFetchSize = runContext.render(fetchSize).as(Integer.class).orElse(10000);

        String sql = rStatusFilter == null
            ? "SELECT * FROM df.list_instances()"
            : "SELECT * FROM df.list_instances() WHERE status = ?";

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            if (rStatusFilter != null) {
                bind(statement, 1, rStatusFilter);
            }
            if (rFetchType == FetchType.STORE) {
                statement.setFetchSize(rFetchSize);
            }

            try (ResultSet rs = statement.executeQuery()) {
                ResultSetMetaData metaData = rs.getMetaData();

                if (rFetchType == FetchType.STORE) {
                    File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
                    long count = 0;
                    try (BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(tempFile))) {
                        while (rs.next()) {
                            FileSerde.write(output, mapRow(rs, metaData));
                            count++;
                        }
                    }
                    URI uri = runContext.storage().putFile(tempFile);
                    runContext.logger().info("Stored {} durable instance(s) to {}", count, uri);
                    return Output.builder().uri(uri).size(count).build();
                }

                List<Map<String, Object>> rows = new ArrayList<>();
                while (rs.next()) {
                    rows.add(mapRow(rs, metaData));
                }
                runContext.logger().info("Fetched {} durable instance(s)", rows.size());
                return Output.builder().instances(rows).size((long) rows.size()).build();
            }
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Matching instances", description = "Only populated when fetchType is FETCH.")
        private final List<Map<String, Object>> instances;

        @Schema(title = "URI of stored results in internal storage", description = "Only populated when fetchType is STORE; stored using ION format.")
        private final URI uri;

        @Schema(title = "Number of matching instances")
        private final Long size;
    }
}
