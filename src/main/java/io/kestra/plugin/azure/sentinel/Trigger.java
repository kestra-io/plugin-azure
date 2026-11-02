package io.kestra.plugin.azure.sentinel;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.models.triggers.TriggerService;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.storages.kv.KVMetadata;
import io.kestra.core.storages.kv.KVStore;
import io.kestra.core.storages.kv.KVValueAndMetadata;
import io.kestra.plugin.azure.shared.AzureIdentityConnectionInterface;

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
    title = "Trigger flows from Microsoft Sentinel incidents",
    description = """
        Emits matching incidents within initialLookback on the first poll and newly modified incidents on subsequent polls,
        up to maxIncidents per execution. A timestamp and boundary versions are persisted in namespace KV.
        Polling observes returned incident states and cannot guarantee exactly-once delivery or capture every intermediate update."""
)
@Plugin(examples = @Example(title = "React to high severity Sentinel incidents", full = true, code = """
    id: sentinel_incidents
    namespace: company.team
    tasks:
      - id: log
        type: io.kestra.plugin.core.log.Log
        message: "Received {{ trigger.count }} Sentinel incidents"
    triggers:
      - id: incidents
        type: io.kestra.plugin.azure.sentinel.Trigger
        tenantId: "{{ secret('AZURE_TENANT_ID') }}"
        clientId: "{{ secret('AZURE_CLIENT_ID') }}"
        clientSecret: "{{ secret('AZURE_CLIENT_SECRET') }}"
        subscriptionId: "{{ secret('AZURE_SUBSCRIPTION_ID') }}"
        resourceGroup: security
        workspaceName: sentinel-workspace
        severities: [High]
        initialLookback: P1D
        maxIncidents: 100
        interval: PT1M
    """))
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Trigger.Output>, AzureIdentityConnectionInterface {
    private static final String ORDER = "properties/lastModifiedTimeUtc asc";

    @Schema(title = "Tenant ID")
    @PluginProperty(group = "connection")
    protected Property<String> tenantId;

    @Schema(title = "Client ID")
    @PluginProperty(group = "connection")
    protected Property<String> clientId;

    @Schema(title = "Client secret")
    @PluginProperty(secret = true, group = "connection")
    @ToString.Exclude
    protected Property<String> clientSecret;

    @Schema(title = "PEM certificate")
    @PluginProperty(secret = true, group = "connection")
    @ToString.Exclude
    protected Property<String> pemCertificate;

    @NotNull
    @Schema(
        title = "Azure subscription ID",
        description = """
            ID of the Azure subscription that contains the Sentinel workspace."""
    )
    @PluginProperty(group = "connection")
    protected Property<String> subscriptionId;

    @NotNull
    @Schema(
        title = "Azure resource group",
        description = """
            Name of the resource group that contains the Sentinel workspace."""
    )
    @PluginProperty(group = "connection")
    protected Property<String> resourceGroup;

    @NotNull
    @Schema(
        title = "Sentinel workspace name",
        description = """
            ARM resource name of the Log Analytics workspace, not its customer GUID (the `workspaceId` used by `Query`)."""
    )
    @PluginProperty(group = "connection")
    protected Property<String> workspaceName;

    @Builder.Default
    @Schema(
        title = "Azure Resource Manager endpoint",
        description = """
            Base URL of the Azure Resource Manager API. Defaults to `https://management.azure.com` (public cloud).
            Override it for sovereign clouds, for example `https://management.usgovcloudapi.net` (Azure Government) or
            `https://management.chinacloudapi.cn` (Azure China). The access token scope is derived from this endpoint
            (`<endpoint>/.default`). Microsoft Entra sign-in uses the public authority
            unless the worker sets the `AZURE_AUTHORITY_HOST` environment variable, for example `https://login.microsoftonline.us`."""
    )
    @PluginProperty(group = "connection")
    protected Property<String> armEndpoint = Property.ofValue(AbstractSentinel.DEFAULT_ARM_ENDPOINT);

    @Schema(
        title = "OData filter",
        description = """
            Raw OData predicate, combined with severity, status, and watermark filters using AND."""
    )
    @PluginProperty(group = "main")
    protected Property<String> filter;

    @Schema(
        title = "Incident severities",
        description = """
            Severities to keep: `High`, `Medium`, `Low` or `Informational`. Values within this list are combined using OR."""
    )
    @PluginProperty(group = "main")
    protected Property<List<IncidentSeverity>> severities;

    @Schema(
        title = "Incident statuses",
        description = """
            Statuses to keep: `New`, `Active` or `Closed`. Values within this list are combined using OR."""
    )
    @PluginProperty(group = "main")
    protected Property<List<IncidentStatus>> statuses;

    @Builder.Default
    @Schema(
        title = "Page size",
        description = """
            Number of incidents requested per page, between 1 and 1000. Defaults to 100.
            `maxIncidents` limits the emitted batch."""
    )
    @PluginProperty(group = "advanced")
    protected Property<@Min(1) @Max(1000) Integer> top = Property.ofValue(100);

    @Builder.Default
    @Schema(
        title = "Initial lookback",
        description = """
            Positive duration, default P1D. Limits the first poll to recently modified incidents; ignored once a watermark exists."""
    )
    @PluginProperty(group = "processing")
    protected Property<Duration> initialLookback = Property.ofValue(Duration.ofDays(1));

    @Builder.Default
    @Schema(
        title = "Maximum incidents",
        description = """
            Maximum number of fresh incidents per execution, between 1 and 1000. Defaults to 100.
            Remaining incidents are eligible on subsequent polls."""
    )
    @PluginProperty(group = "processing")
    protected Property<@Min(1) @Max(1000) Integer> maxIncidents = Property.ofValue(100);

    @Builder.Default
    @Schema(title = "Polling interval")
    @PluginProperty(group = "advanced")
    private final Duration interval = Duration.ofMinutes(1);

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        var runContext = conditionContext.getRunContext();
        var rInitialLookback = runContext.render(initialLookback).as(Duration.class).orElse(Duration.ofDays(1));
        var rMaxIncidents = runContext.render(maxIncidents).as(Integer.class).orElse(100);
        if (rInitialLookback.isZero() || rInitialLookback.isNegative()) {
            throw new IllegalArgumentException("initialLookback must be positive");
        }
        if (rMaxIncidents <= 0) {
            throw new IllegalArgumentException("maxIncidents must be positive");
        }
        var kv = runContext.namespaceKv(context.getNamespace());
        var key = stateKey(context.getFlowId(), getId());
        var previous = load(kv, key);
        var rFilter = runContext.render(filter).as(String.class).orElse(null);
        var incidents = ListIncidents.builder()
            .id(id).type(ListIncidents.class.getName())
            .tenantId(tenantId).clientId(clientId).clientSecret(clientSecret).pemCertificate(pemCertificate)
            .subscriptionId(subscriptionId).resourceGroup(resourceGroup).workspaceName(workspaceName)
            .armEndpoint(armEndpoint).severities(severities).statuses(statuses).top(top).build();
        var poll = poll(previous, Instant.now(), rInitialLookback, rMaxIncidents, rFilter, (pollFilter, consumer) -> incidents.incidentPages(runContext, pollFilter, ORDER, consumer));
        if (poll.fresh().isEmpty()) {
            return Optional.empty();
        }
        var execution = TriggerService.generateExecution(this, conditionContext, context, Output.builder().incidents(poll.fresh()).count((long) poll.fresh().size()).build());
        save(kv, key, poll.next());
        return Optional.of(execution);
    }

    @FunctionalInterface
    interface PageSource {
        void pages(String filter, AbstractSentinel.PageConsumer consumer) throws Exception;
    }

    record Poll(List<Map<String, Object>> fresh, Watermark next) {
    }

    /** Pure selection of the incidents to emit and the watermark to persist once they have been delivered. */
    static Poll poll(Watermark previous, Instant now, Duration lookback, int limit, String userFilter, PageSource source) throws Exception {
        var lowerBound = previous.timestamp() == null ? now.minus(lookback) : previous.timestamp();
        var watermarkFilter = "properties/lastModifiedTimeUtc ge " + lowerBound;
        var pollFilter = userFilter == null || userFilter.isBlank() ? watermarkFilter : "(" + userFilter + ") and (" + watermarkFilter + ")";
        var fresh = new ArrayList<Map<String, Object>>();
        var seen = new HashSet<Version>();
        source.pages(pollFilter, page ->
        {
            for (var incident : page) {
                var version = version(incident);
                // Apply the exact API/lookback cutoff before normalizing the persisted identity.
                if (version.modified().isBefore(lowerBound)) {
                    continue;
                }
                var normalized = version.atMicrosecondPrecision();
                if (previous.boundary().contains(normalized) || !seen.add(normalized)) {
                    continue;
                }
                fresh.add(incident);
                if (fresh.size() == limit) {
                    return false;
                }
            }
            return true;
        });
        return new Poll(fresh, advance(previous, fresh));
    }

    static Watermark advance(Watermark previous, List<Map<String, Object>> fresh) {
        var maximum = previous.timestamp();
        var boundary = new HashSet<Version>(previous.boundary());
        for (var incident : fresh) {
            var version = version(incident).atMicrosecondPrecision();
            if (maximum == null || version.modified().isAfter(maximum)) {
                maximum = version.modified();
                boundary.clear();
            }
            if (version.modified().equals(maximum)) {
                boundary.add(version);
            }
        }
        return new Watermark(maximum, boundary);
    }

    static Watermark load(KVStore kv, String key) throws Exception {
        var stored = kv.getValue(key);
        var watermark = stored.isPresent()
            ? JacksonMapper.ofJson().readValue((byte[]) stored.get().value(), Watermark.class)
            : new Watermark(null, Set.of());
        if (watermark.boundary() == null) {
            throw new IllegalStateException("Invalid Sentinel watermark: missing boundary versions");
        }
        return watermark;
    }

    static void save(KVStore kv, String key, Watermark watermark) throws Exception {
        kv.put(key, new KVValueAndMetadata(new KVMetadata("Sentinel incident watermark", (Duration) null), JacksonMapper.ofJson().writeValueAsBytes(watermark)));
    }

    static String stateKey(String flowId, String triggerId) {
        return "sentinel_watermark_" + flowId.length() + "_" + flowId + "_" + triggerId;
    }

    private static Version version(Map<String, Object> incident) {
        var identity = incident.get("id");
        if (
            !(identity instanceof String name) || name.isBlank() || !(incident.get("properties") instanceof Map<?, ?> properties)
                || !(properties.get("lastModifiedTimeUtc") instanceof String modified)
        ) {
            throw new IllegalArgumentException("Sentinel incident is missing its id or lastModifiedTimeUtc");
        }
        return new Version(name, Instant.parse(modified), (String) incident.get("etag"));
    }

    record Version(String id, Instant modified, String etag) {
        Version atMicrosecondPrecision() {
            // Kestra 2.0 serializes Instant values with six fractional digits in namespace KV.
            return new Version(id, modified.truncatedTo(ChronoUnit.MICROS), etag);
        }
    }

    record Watermark(Instant timestamp, Set<Version> boundary) {
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "New or modified incidents")
        private final List<Map<String, Object>> incidents;
        @Schema(title = "Incident count")
        private final Long count;
    }
}
