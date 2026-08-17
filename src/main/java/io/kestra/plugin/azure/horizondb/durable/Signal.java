package io.kestra.plugin.azure.horizondb.durable;

import java.sql.Connection;
import java.sql.PreparedStatement;

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
    description = "Sends a named signal (with an optional payload) to an instance waiting on `df.wait_for_signal(...)`, via `SELECT df.signal(id, name, data)`."
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

    @Schema(title = "Signal name", description = "Name of the external signal the instance is waiting on (matches its df.wait_for_signal(name) call).")
    @NotNull
    @PluginProperty(group = "main")
    protected Property<String> signalName;

    @Schema(
        title = "Signal payload",
        description = "Payload passed to the instance as the signal's data, typically a JSON string. Defaults to '{}', matching df.signal()'s own default."
    )
    @Builder.Default
    @PluginProperty(group = "main")
    protected Property<String> payload = Property.ofValue("{}");

    @Override
    protected Output run(RunContext runContext, Connection connection) throws Exception {
        String rInstanceId = runContext.render(instanceId).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("instanceId is required"));
        String rSignalName = runContext.render(signalName).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("signalName is required"));
        String rPayload = runContext.render(payload).as(String.class).orElse("{}");

        // df.signal(id text, name text, data text) — no confirmed return value/shape is
        // documented, so this only asserts the call completed without the driver throwing; it
        // does not assume a specific result column.
        try (PreparedStatement statement = connection.prepareStatement("SELECT df.signal(?, ?, ?)")) {
            trackStatement(statement);
            bind(statement, 1, rInstanceId);
            bind(statement, 2, rSignalName);
            bind(statement, 3, rPayload);
            statement.execute();
        }

        runContext.logger().info("Signal '{}' sent to instance {}", rSignalName, rInstanceId);
        return Output.builder().instanceId(rInstanceId).signaled(true).build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Durable instance id")
        private final String instanceId;

        @Schema(
            title = "Whether the signal was sent",
            description = "True once df.signal() executes without error. This does not confirm a waiting instance actually received it (e.g. if it wasn't waiting on that signal name) — check GetStatus or df.explain() separately if you need to verify delivery."
        )
        private final Boolean signaled;
    }
}
