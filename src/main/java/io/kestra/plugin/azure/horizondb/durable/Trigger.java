package io.kestra.plugin.azure.horizondb.durable;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.models.triggers.StatefulTriggerInterface;
import io.kestra.core.models.triggers.StatefulTriggerService;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.models.triggers.TriggerService;
import io.kestra.core.runners.RunContext;

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
    title = "Trigger a flow when a pg_durable instance reaches a target status",
    description = "Polls `df.list_instances()` on an interval and starts an execution the first time one or more instances are newly seen in, or newly transition to, targetStatus. Already-seen instances that remain in targetStatus do not refire; state is persisted in the flow's namespace KV store. Each poll opens and closes its own JDBC connection, so interval should not be set unreasonably low."
)
@Plugin(
    examples = {
        @Example(
            title = "Trigger a flow when a durable function instance completes",
            full = true,
            code = """
                id: horizondb_durable_on_completion
                namespace: company.team

                triggers:
                  - id: on_durable_complete
                    type: io.kestra.plugin.azure.horizondb.durable.Trigger
                    host: "{{ secret('HORIZONDB_HOST') }}"
                    port: 5432
                    database: mydb
                    username: "{{ secret('HORIZONDB_USERNAME') }}"
                    password: "{{ secret('HORIZONDB_PASSWORD') }}"
                    targetStatus: Completed
                    interval: PT30S

                tasks:
                  - id: log_completion
                    type: io.kestra.plugin.core.log.Log
                    message: "Durable instance {{ trigger.instances[0].instance_id }} completed"
                """
        )
    }
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<ListInstances.Output>, StatefulTriggerInterface {
    @Schema(title = "HorizonDB server host", description = "Hostname of the Azure HorizonDB server, without protocol or port.")
    @NotNull
    @PluginProperty(group = "connection")
    protected Property<String> host;

    @Schema(title = "HorizonDB server port", description = "Defaults to the standard PostgreSQL port.")
    @Builder.Default
    @PluginProperty(group = "connection")
    protected Property<Integer> port = Property.ofValue(5432);

    @Schema(title = "Database name")
    @NotNull
    @PluginProperty(group = "connection")
    protected Property<String> database;

    @Schema(title = "Username", description = "Required unless useEntraId is true and the Entra ID token carries the identity.")
    @PluginProperty(group = "connection")
    protected Property<String> username;

    @Schema(title = "Password", description = "Required unless useEntraId is true.")
    @PluginProperty(secret = true, group = "connection")
    @ToString.Exclude
    protected Property<String> password;

    @Schema(
        title = "Authenticate with Azure Entra ID",
        description = "When true, authenticates using Azure Entra ID instead of a static password. With no further properties set, this falls back to whatever DefaultAzureCredential resolves on the worker; set tenantId/clientId/clientSecret below to authenticate as a specific service principal instead."
    )
    @Builder.Default
    @PluginProperty(group = "connection")
    protected Property<Boolean> useEntraId = Property.ofValue(false);

    @Schema(title = "Azure tenant id", description = "Used with clientId/clientSecret for service principal authentication when useEntraId is true. Ignored otherwise.")
    @PluginProperty(group = "connection")
    protected Property<String> tenantId;

    @Schema(title = "Azure client id", description = "Used with tenantId/clientSecret for service principal authentication when useEntraId is true. Ignored otherwise.")
    @PluginProperty(group = "connection")
    protected Property<String> clientId;

    @Schema(title = "Azure client secret", description = "Used with tenantId/clientId for service principal authentication when useEntraId is true. Ignored otherwise.")
    @PluginProperty(secret = true, group = "connection")
    @ToString.Exclude
    protected Property<String> clientSecret;

    @Schema(title = "Require TLS", description = "When true (the default), each poll's connection is rejected unless it is encrypted (`sslmode=require`).")
    @Builder.Default
    @PluginProperty(group = "connection")
    protected Property<Boolean> ssl = Property.ofValue(true);

    @Schema(
        title = "Target status",
        description = "Instance status to watch for (e.g. Completed, Failed, Cancelled)."
    )
    @NotNull
    @PluginProperty(group = "main")
    protected Property<String> targetStatus;

    @Schema(title = "Polling interval", description = "How often to poll df.list_instances(). Defaults to 60 seconds.")
    @Builder.Default
    @PluginProperty(group = "main")
    private final Duration interval = Duration.ofSeconds(60);

    @Schema(
        title = "State transition to fire on",
        description = "CREATE_OR_UPDATE (the default) fires when an instance id is newly seen in targetStatus, or newly transitions into it; other StatefulTriggerInterface.On values follow the same semantics as file-based stateful triggers."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    protected Property<On> on = Property.ofValue(On.CREATE_OR_UPDATE);

    @Schema(
        title = "State store key",
        description = "Namespace KV key used to persist which instance ids/statuses have already fired. Defaults to a key derived from the trigger's namespace, flow id, and trigger id."
    )
    @PluginProperty(group = "advanced")
    protected Property<String> stateKey;

    @Schema(
        title = "State entry time-to-live",
        description = "How long a seen instance id is remembered in the state store. Unset means entries are kept indefinitely."
    )
    @PluginProperty(group = "advanced")
    protected Property<Duration> stateTtl;

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();

        String rTargetStatus = runContext.render(this.targetStatus).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("targetStatus is required"));

        ListInstances.Output output = pollInstances(runContext, rTargetStatus);

        List<Map<String, Object>> instances = output.getInstances() == null ? List.of() : output.getInstances();
        logger.debug("Polled {} instance(s) in status {}", instances.size(), rTargetStatus);

        if (instances.isEmpty()) {
            return Optional.empty();
        }

        On rOn = runContext.render(this.on).as(On.class).orElse(On.CREATE_OR_UPDATE);
        String rStateKey = runContext.render(this.stateKey).as(String.class)
            .orElse(StatefulTriggerService.defaultKey(context.getNamespace(), context.getFlowId(), this.getId()));
        Optional<Duration> rStateTtl = runContext.render(this.stateTtl).as(Duration.class);

        Map<String, StatefulTriggerService.Entry> state = StatefulTriggerService.readState(runContext, rStateKey, rStateTtl);

        List<Map<String, Object>> fired = computeFired(instances, state, rOn);

        StatefulTriggerService.writeState(runContext, rStateKey, state, rStateTtl);

        if (fired.isEmpty()) {
            return Optional.empty();
        }

        logger.info("{} durable instance(s) newly reached status {}", fired.size(), rTargetStatus);

        ListInstances.Output fireOutput = ListInstances.Output.builder()
            .instances(fired)
            .size((long) fired.size())
            .build();

        return Optional.of(
            TriggerService.generateExecution(this, conditionContext, context, fireOutput)
        );
    }

    /**
     * Opens a connection and runs {@code df.list_instances()} filtered to {@code targetStatus}.
     * Extracted from {@link #evaluate} (rather than inlined) so tests can override just this
     * database-touching step — e.g. to return a canned {@link ListInstances.Output} — and
     * exercise the rest of {@code evaluate}'s logic (state read/write, dedup, execution
     * generation) without a live HorizonDB instance.
     */
    protected ListInstances.Output pollInstances(RunContext runContext, String targetStatus) throws Exception {
        return ListInstances.builder()
            .id(this.id)
            .type(ListInstances.class.getName())
            .host(this.host)
            .port(this.port)
            .database(this.database)
            .username(this.username)
            .password(this.password)
            .useEntraId(this.useEntraId)
            .tenantId(this.tenantId)
            .clientId(this.clientId)
            .clientSecret(this.clientSecret)
            .ssl(this.ssl)
            .statusFilter(Property.ofValue(targetStatus))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build()
            .run(runContext);
    }

    /**
     * Applies {@link StatefulTriggerService} change detection to the polled instances, mutating
     * {@code state} in place (as {@link StatefulTriggerService#computeAndUpdateState} does) and
     * returning only the instances that should fire an execution. Extracted as a pure function
     * of its inputs (no I/O) so the dedup semantics can be unit tested without a live database.
     */
    static List<Map<String, Object>> computeFired(
        List<Map<String, Object>> instances,
        Map<String, StatefulTriggerService.Entry> state,
        On on
    ) {
        List<Map<String, Object>> fired = new ArrayList<>();
        for (Map<String, Object> instance : instances) {
            Object instanceId = instance.get("instance_id");
            if (instanceId == null) {
                continue;
            }
            Object status = instance.get("status");
            var candidate = StatefulTriggerService.Entry.candidate(
                String.valueOf(instanceId),
                status == null ? null : String.valueOf(status),
                Instant.now()
            );
            StatefulTriggerService.StateUpdate update = StatefulTriggerService.computeAndUpdateState(state, candidate, on);
            if (update.fire()) {
                fired.add(instance);
            }
        }
        return fired;
    }
}
