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
    title = "Get the status and result of a pg_durable instance",
    description = "Returns the current status and, if completed, the result of a durable instance via `df.status()` / `df.result()`."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: horizondb_durable_get_status
                namespace: company.team

                inputs:
                  - id: host
                    type: STRING
                  - id: instance_id
                    type: STRING

                tasks:
                  - id: poll_status
                    type: io.kestra.plugin.azure.horizondb.durable.GetStatus
                    host: "{{ inputs.host }}"
                    port: 5432
                    database: mydb
                    username: "{{ secret('HORIZONDB_USERNAME') }}"
                    password: "{{ secret('HORIZONDB_PASSWORD') }}"
                    instanceId: "{{ inputs.instance_id }}"
                """
        )
    }
)
public class GetStatus extends AbstractHorizonDb<GetStatus.Output> implements RunnableTask<GetStatus.Output> {
    @Schema(title = "Durable instance id", description = "Identifier of the instance to query.")
    @NotNull
    @PluginProperty(group = "main")
    protected Property<String> instanceId;

    @Override
    protected Output run(RunContext runContext, Connection connection) throws Exception {
        String rInstanceId = runContext.render(instanceId).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("instanceId is required"));

        try (PreparedStatement statement = connection.prepareStatement("SELECT df.status(?) AS status, df.result(?) AS result")) {
            trackStatement(statement);
            bind(statement, 1, rInstanceId);
            bind(statement, 2, rInstanceId);

            try (ResultSet rs = statement.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalStateException("No status found for instance " + rInstanceId);
                }
                String status = rs.getString("status");
                String result = rs.getString("result");
                runContext.logger().info("Instance {} status={}", rInstanceId, status);
                return Output.builder().instanceId(rInstanceId).status(status).result(result).build();
            }
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Durable instance id")
        private final String instanceId;

        @Schema(title = "Current status", description = "e.g. Running, Completed, Failed, Cancelled.")
        private final String status;

        @Schema(title = "Instance result", description = "Populated once the instance reaches a terminal state; null while running.")
        private final String result;
    }
}
