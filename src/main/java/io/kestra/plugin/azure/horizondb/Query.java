package io.kestra.plugin.azure.horizondb;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
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
    title = "Execute a single SQL query against Azure HorizonDB",
    description = "Runs one SQL statement over a JDBC connection and returns results according to fetchType (FETCH, FETCH_ONE, STORE, or NONE)."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: horizondb_query
                namespace: company.team

                inputs:
                  - id: host
                    type: STRING

                tasks:
                  - id: query
                    type: io.kestra.plugin.azure.horizondb.Query
                    host: "{{ inputs.host }}"
                    port: 5432
                    database: mydb
                    username: "{{ secret('HORIZONDB_USERNAME') }}"
                    password: "{{ secret('HORIZONDB_PASSWORD') }}"
                    sql: |
                      SELECT id, total, status FROM orders WHERE loaded_at > now() - INTERVAL '1 day'
                    fetchType: STORE
                """
        )
    },
    metrics = {
        @Metric(name = "fetch.size", type = Counter.TYPE, unit = "rows", description = "The number of fetched or stored rows.")
    }
)
public class Query extends AbstractHorizonDb<Query.Output> implements RunnableTask<Query.Output> {
    @Schema(
        title = "SQL statement to execute",
        description = "A single SQL statement, rendered with flow variables before execution."
    )
    @NotNull
    @PluginProperty(group = "main")
    protected Property<String> sql;

    @Schema(
        title = "Result fetching mode",
        description = "FETCH returns all rows, FETCH_ONE returns the first row only, STORE streams rows to internal storage (ION), NONE returns no rows."
    )
    @NotNull
    @Builder.Default
    @PluginProperty(group = "main")
    protected Property<FetchType> fetchType = Property.ofValue(FetchType.NONE);

    @Schema(
        title = "JDBC fetch size",
        description = "Number of rows fetched per database round trip; only used for STORE mode."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    protected Property<@Min(1) @Max(100000) Integer> fetchSize = Property.ofValue(10000);

    @Override
    protected Output run(RunContext runContext, Connection connection) throws Exception {
        String rSql = runContext.render(sql).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("sql is required"));
        FetchType rFetchType = runContext.render(fetchType).as(FetchType.class).orElse(FetchType.NONE);
        Integer rFetchSize = runContext.render(fetchSize).as(Integer.class).orElse(10000);

        Output output;
        try (Statement statement = createStatement(connection)) {
            trackStatement(statement);

            if (rFetchType == FetchType.STORE) {
                statement.setFetchSize(rFetchSize);
            }

            boolean isResultSet = statement.execute(rSql);

            if (!isResultSet) {
                long updateCount = statement.getUpdateCount();
                runContext.logger().info("Executed statement, {} row(s) affected", updateCount);
                return Output.builder().updateCount(updateCount).build();
            }

            try (ResultSet rs = statement.getResultSet()) {
                output = fetch(runContext, rs, rFetchType);
            }
        }

        if (output.getSize() != null) {
            runContext.metric(Counter.of("fetch.size", output.getSize()));
        }

        return output;
    }

    static Output fetch(RunContext runContext, ResultSet rs, FetchType fetchType) throws Exception {
        ResultSetMetaData metaData = rs.getMetaData();

        switch (fetchType) {
            case FETCH_ONE -> {
                if (rs.next()) {
                    return Output.builder().row(mapRow(rs, metaData)).size(1L).build();
                }
                return Output.builder().size(0L).build();
            }
            case FETCH -> {
                List<Map<String, Object>> rows = new ArrayList<>();
                while (rs.next()) {
                    rows.add(mapRow(rs, metaData));
                }
                return Output.builder().rows(rows).size((long) rows.size()).build();
            }
            case STORE -> {
                File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
                long count = 0;
                try (BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(tempFile))) {
                    while (rs.next()) {
                        FileSerde.write(output, mapRow(rs, metaData));
                        count++;
                    }
                }
                URI uri = runContext.storage().putFile(tempFile);
                return Output.builder().uri(uri).size(count).build();
            }
            default -> {
                return Output.builder().build();
            }
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "First row of fetched data", description = "Only populated when fetchType is FETCH_ONE.")
        private final Map<String, Object> row;

        @Schema(title = "All fetched rows", description = "Only populated when fetchType is FETCH.")
        private final List<Map<String, Object>> rows;

        @Schema(title = "URI of stored results in internal storage", description = "Only populated when fetchType is STORE; stored using ION format.")
        private final URI uri;

        @Schema(title = "Number of rows fetched or stored", description = "Populated when fetchType is FETCH, FETCH_ONE, or STORE.")
        private final Long size;

        @Schema(title = "Rows affected", description = "Populated when the statement did not return a result set (e.g. INSERT, UPDATE, DELETE, DDL).")
        private final Long updateCount;
    }
}
