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
    title = "Send an external signal to a pg_durable instance",
    description = "Sends a named signal (with an optional payload) to a waiting instance via `SELECT df.signal(...)`."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: horizondb_durable_signal
                namespace: company.team

                inputs:
                  - id: host
                    type: STRING
                  - id: instance_id
                    type: STRING

                tasks:
                  - id: approve
                    type: io.kestra.plugin.azure.horizondb.durable.Signal
                    host: "{{ inputs.host }}"
                    port: 5432
                    database: mydb
                    username: "{{ secret('HORIZONDB_USERNAME') }}"
                    password: "{{ secret('HORIZONDB_PASSWORD') }}"
                    instanceId: "{{ inputs.instance_id }}"
                    signalName: approval
                    payload: '{"approved": true, "approver": "jane"}'
                """
        )
    }
)
public class Signal extends AbstractHorizonDb<Signal.Output> implements RunnableTask<Signal.Output> {
    @Schema(title = "Durable instance id", description = "Identifier of the instance to signal.")
    @NotNull
    @PluginProperty(group = "main")
    protected Property<String> instanceId;

    @Schema(title = "Signal name", description = "Name of the external signal the instance is waiting on.")
    @NotNull
    @PluginProperty(group = "main")
    protected Property<String> signalName;

    @Schema(title = "Signal payload", description = "Optional payload passed to the instance, typically a JSON string.")
    @PluginProperty(group = "main")
    protected Property<String> payload;

    @Override
    protected Output run(RunContext runContext, Connection connection) throws Exception {
        String rInstanceId = runContext.render(instanceId).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("instanceId is required"));
        String rSignalName = runContext.render(signalName).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("signalName is required"));
        String rPayload = runContext.render(payload).as(String.class).orElse(null);

        try (PreparedStatement statement = connection.prepareStatement("SELECT df.signal(?, ?, ?) AS signaled")) {
            bind(statement, 1, rInstanceId);
            bind(statement, 2, rSignalName);
            bind(statement, 3, rPayload);

            try (ResultSet rs = statement.executeQuery()) {
                boolean signaled = rs.next() && rs.getBoolean("signaled");
                runContext.logger().info("Signal '{}' for instance {} returned signaled={}", rSignalName, rInstanceId, signaled);
                return Output.builder().instanceId(rInstanceId).signaled(signaled).build();
            }
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Durable instance id")
        private final String instanceId;

        @Schema(title = "Whether the signal was accepted")
        private final Boolean signaled;
    }
}
