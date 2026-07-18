package io.kestra.plugin.azure.horizondb;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
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
    title = "Execute multiple SQL statements against Azure HorizonDB",
    description = "Runs a sequence of semicolon-separated SQL statements over a single JDBC connection and returns per-statement outputs in order."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: horizondb_queries
                namespace: company.team

                inputs:
                  - id: host
                    type: STRING

                tasks:
                  - id: queries
                    type: io.kestra.plugin.azure.horizondb.Queries
                    host: "{{ inputs.host }}"
                    port: 5432
                    database: mydb
                    username: "{{ secret('HORIZONDB_USERNAME') }}"
                    password: "{{ secret('HORIZONDB_PASSWORD') }}"
                    sql: |
                      DELETE FROM staging WHERE loaded_at < now() - INTERVAL '1 day';
                      INSERT INTO target SELECT * FROM staging;
                    fetchType: NONE
                """
        )
    }
)
public class Queries extends AbstractHorizonDb<Queries.Output> implements RunnableTask<Queries.Output> {
    @Schema(
        title = "SQL statements to execute",
        description = "One or more SQL statements separated by semicolons, rendered with flow variables before execution and run in order over the same connection."
    )
    @NotNull
    @PluginProperty(group = "main")
    protected Property<String> sql;

    @Schema(
        title = "Result fetching mode",
        description = "Applied uniformly to every statement in the sequence. FETCH returns all rows, FETCH_ONE returns the first row only, STORE streams rows to internal storage (ION), NONE returns no rows."
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

        List<String> statements = splitStatements(rSql);
        List<Query.Output> outputs = new ArrayList<>();

        try (Statement statement = createStatement(connection)) {
            if (rFetchType == FetchType.STORE) {
                statement.setFetchSize(rFetchSize);
            }

            for (String single : statements) {
                boolean isResultSet = statement.execute(single);

                if (!isResultSet) {
                    outputs.add(Query.Output.builder().updateCount((long) statement.getUpdateCount()).build());
                } else {
                    try (ResultSet rs = statement.getResultSet()) {
                        outputs.add(Query.fetch(runContext, rs, rFetchType));
                    }
                }
            }
        }

        return Output.builder().outputs(outputs).build();
    }

    /**
     * Splits on statement-terminating semicolons. Does not attempt to parse quoted strings or
     * dollar-quoted blocks, so semicolons embedded inside string literals will incorrectly split
     * the statement; keep such SQL in a single Query task if that is a concern.
     */
    static List<String> splitStatements(String sql) {
        List<String> statements = new ArrayList<>();
        for (String part : sql.split(";")) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                statements.add(trimmed);
            }
        }
        return statements;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Per-statement outputs", description = "One entry per executed statement, in order.")
        private final List<Query.Output> outputs;
    }
}
