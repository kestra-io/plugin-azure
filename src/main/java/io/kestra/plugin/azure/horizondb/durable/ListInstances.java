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
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
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
    description = "Queries `df.list_instances(status, limit)`, with an optional status filter and row limit, and returns matching instances according to fetchType (FETCH or STORE)."
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
        description = "Optional status to filter instances by (e.g. Running, Completed, Failed, Cancelled). Passed as df.list_instances()'s own status argument. When empty, all instances (up to limit) are returned."
    )
    @PluginProperty(group = "main")
    protected Property<String> statusFilter;

    @Schema(
        title = "Row limit",
        description = "Maximum number of instances to return, passed as df.list_instances()'s own limit argument. Defaults to 1000 so this can't silently load an unbounded number of instances into memory (or into a STORE'd file); raise it explicitly if you need more in one call."
    )
    @Builder.Default
    @PluginProperty(group = "main")
    protected Property<@Min(1) @Max(10000) Integer> limit = Property.ofValue(1000);

    @Schema(
        title = "Result fetching mode",
        description = "STORE streams matching instances to internal storage (ION). Any other value (including the FETCH default) returns them as an in-memory list; FETCH_ONE and NONE are not treated specially by this task."
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
    protected Property<@Min(1) @Max(100000) Integer> fetchSize = Property.ofValue(10000);

    @Override
    protected Output run(RunContext runContext, Connection connection) throws Exception {
        String rStatusFilter = runContext.render(statusFilter).as(String.class).orElse(null);
        Integer rLimit = runContext.render(limit).as(Integer.class).orElse(1000);
        FetchType rFetchType = runContext.render(fetchType).as(FetchType.class).orElse(FetchType.FETCH);
        Integer rFetchSize = runContext.render(fetchSize).as(Integer.class).orElse(10000);

        // df.list_instances(p_status text DEFAULT NULL, p_limit integer DEFAULT NULL) takes the
        // filter and limit as its own arguments rather than being wrapped in an external WHERE
        // clause, matching the documented signature and Quick Reference examples:
        //   SELECT * FROM df.list_instances();
        //   SELECT * FROM df.list_instances('Running');
        //   SELECT * FROM df.list_instances(NULL, 10);
        try (PreparedStatement statement = connection.prepareStatement("SELECT * FROM df.list_instances(?, ?)")) {
            trackStatement(statement);
            bind(statement, 1, rStatusFilter);
            bind(statement, 2, rLimit, java.sql.Types.INTEGER);
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
