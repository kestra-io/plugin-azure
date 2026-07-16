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
    title = "Cancel a pg_durable durable function instance",
    description = "Cancels a running instance via `SELECT df.cancel(...)`."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: horizondb_durable_cancel
                namespace: company.team

                inputs:
                  - id: host
                    type: STRING
                  - id: instance_id
                    type: STRING

                tasks:
                  - id: cancel
                    type: io.kestra.plugin.azure.horizondb.durable.Cancel
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
public class Cancel extends AbstractHorizonDb<Cancel.Output> implements RunnableTask<Cancel.Output> {
    @Schema(title = "Durable instance id", description = "Identifier of the instance to cancel.")
    @NotNull
    @PluginProperty(group = "main")
    protected Property<String> instanceId;

    @Override
    protected Output run(RunContext runContext, Connection connection) throws Exception {
        String rInstanceId = runContext.render(instanceId).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("instanceId is required"));

        try (PreparedStatement statement = connection.prepareStatement("SELECT df.cancel(?) AS cancelled")) {
            bind(statement, 1, rInstanceId);

            try (ResultSet rs = statement.executeQuery()) {
                boolean cancelled = rs.next() && rs.getBoolean("cancelled");
                runContext.logger().info("Cancel request for instance {} returned cancelled={}", rInstanceId, cancelled);
                return Output.builder().instanceId(rInstanceId).cancelled(cancelled).build();
            }
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Durable instance id")
        private final String instanceId;

        @Schema(title = "Whether the cancel request was accepted")
        private final Boolean cancelled;
    }
}
