package io.kestra.plugin.azure.horizondb.durable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.horizondb.AbstractHorizonDb;
import io.swagger.v3.oas.annotations.media.Schema;
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
    title = "Start a pg_durable durable function instance",
    description = "Submits a pg_durable function body via `SELECT df.start(func, label, database)` and returns the new instance id."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: horizondb_durable_start
                namespace: company.team

                inputs:
                  - id: host
                    type: STRING

                tasks:
                  - id: start_etl
                    type: io.kestra.plugin.azure.horizondb.durable.Start
                    host: "{{ inputs.host }}"
                    port: 5432
                    database: mydb
                    username: "{{ secret('HORIZONDB_USERNAME') }}"
                    password: "{{ secret('HORIZONDB_PASSWORD') }}"
                    functionBody: |
                      'DELETE FROM target WHERE loaded_at < now() - INTERVAL ''1 day'''
                      ~> 'INSERT INTO target SELECT * FROM staging'
                      ~> 'REINDEX TABLE target'
                      ~> 'INSERT INTO etl_log (job, finished_at) VALUES (''nightly'', now())'
                    label: nightly-etl
                """
        )
    }
)
public class Start extends AbstractHorizonDb<Start.Output> implements RunnableTask<Start.Output> {
    @Schema(
        title = "Durable function body",
        description = "The pg_durable SQL DSL body describing the sequence of steps to run durably."
    )
    @NotNull
    @PluginProperty(group = "main")
    protected Property<String> functionBody;

    @Schema(
        title = "Instance label",
        description = "Optional human-readable label attached to the instance, useful for filtering with durable.ListInstances."
    )
    @PluginProperty(group = "main")
    protected Property<String> label;

    @Schema(
        title = "Target database",
        description = "Optional database (on the same PostgreSQL cluster) that the function's SQL steps should run against. Omit to run in the extension's own database — the default HorizonDB connection's database is unaffected either way, since this only changes where df.start() executes the function's SQL, not the connection used to submit it."
    )
    @PluginProperty(group = "main")
    protected Property<String> targetDatabase;

    @Override
    protected Output run(RunContext runContext, Connection connection) throws Exception {
        String rFunctionBody = runContext.render(functionBody).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("functionBody is required"));
        String rLabel = runContext.render(label).as(String.class).orElse(null);
        String rTargetDatabase = runContext.render(targetDatabase).as(String.class).orElse(null);

        try (PreparedStatement statement = connection.prepareStatement("SELECT df.start(?, ?, ?) AS instance_id")) {
            bind(statement, 1, rFunctionBody);
            bind(statement, 2, rLabel);
            bind(statement, 3, rTargetDatabase);

            try (ResultSet rs = statement.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalStateException("df.start(...) did not return an instance id");
                }
                String instanceId = rs.getString("instance_id");
                runContext.logger().info("Started durable instance {}", instanceId);
                return Output.builder().instanceId(instanceId).build();
            }
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Durable instance id", description = "Identifier of the newly started pg_durable instance.")
        private final String instanceId;
    }
}
