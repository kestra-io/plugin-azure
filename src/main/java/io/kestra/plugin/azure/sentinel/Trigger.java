package io.kestra.plugin.azure.sentinel;

import java.time.Duration;
import java.time.Instant;
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
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.storages.kv.KVMetadata;
import io.kestra.core.storages.kv.KVValueAndMetadata;
import io.kestra.plugin.azure.shared.AzureIdentityConnectionInterface;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger flows from Microsoft Sentinel incidents",
    description = "Emits matching incidents within initialLookback on the first poll and newly modified incidents on subsequent polls, up to maxIncidents per execution. A timestamp and boundary versions are persisted in namespace KV. Polling observes returned incident states and cannot guarantee exactly-once delivery or capture every intermediate update."
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
    @Schema(title = "Subscription ID")
    @PluginProperty(group = "connection")
    protected Property<String> subscriptionId;
    @NotNull
    @Schema(title = "Resource group")
    @PluginProperty(group = "connection")
    protected Property<String> resourceGroup;
    @NotNull
    @Schema(title = "Log Analytics workspace name")
    @PluginProperty(group = "connection")
    protected Property<String> workspaceName;
    @Schema(title = "OData filter", description = "Combined with severity, status, and watermark filters using AND.")
    @PluginProperty(group = "main")
    protected Property<String> filter;
    @Schema(title = "Incident severities")
    @PluginProperty(group = "main")
    protected Property<List<String>> severities;
    @Schema(title = "Incident statuses")
    @PluginProperty(group = "main")
    protected Property<List<String>> statuses;
    @Builder.Default
    @Schema(title = "Page size", description = "Between 1 and 1000. Controls page size; maxIncidents limits the emitted batch.")
    @PluginProperty(group = "advanced")
    protected Property<Integer> top = Property.ofValue(100);
    @Builder.Default
    @Schema(title = "Initial lookback", description = "Positive duration, default P1D. Limits the first poll to recently modified incidents; ignored once a watermark exists.")
    @PluginProperty(group = "processing")
    protected Property<Duration> initialLookback = Property.ofValue(Duration.ofDays(1));
    @Builder.Default
    @Schema(title = "Maximum incidents", description = "Positive maximum number of fresh incidents per execution, default 100. Remaining incidents are eligible on subsequent polls.")
    @PluginProperty(group = "processing")
    protected Property<Integer> maxIncidents = Property.ofValue(100);
    @Builder.Default
    @Schema(title = "Polling interval")
    @PluginProperty(group = "advanced")
    private final Duration interval = Duration.ofMinutes(1);

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Duration lookback = runContext.render(initialLookback).as(Duration.class).orElse(Duration.ofDays(1));
        int limit = runContext.render(maxIncidents).as(Integer.class).orElse(100);
        if (lookback.isZero() || lookback.isNegative()) {
            throw new IllegalArgumentException("initialLookback must be positive");
        }
        if (limit <= 0) {
            throw new IllegalArgumentException("maxIncidents must be positive");
        }
        var kv = runContext.namespaceKv(context.getNamespace());
        String key = stateKey(context.getFlowId(), getId());
        var stored = kv.getValue(key);
        Watermark previous = stored.isPresent()
            ? JacksonMapper.ofJson().readValue((byte[]) stored.get().value(), Watermark.class)
            : new Watermark(null, Set.of());
        if (previous.boundary() == null) {
            throw new IllegalStateException("Invalid Sentinel watermark: missing boundary versions");
        }
        String combinedFilter = runContext.render(filter).as(String.class).orElse(null);
        Instant lowerBound = previous.timestamp() == null ? now().minus(lookback) : previous.timestamp();
        String watermarkFilter = "properties/lastModifiedTimeUtc ge " + lowerBound;
        combinedFilter = combinedFilter == null || combinedFilter.isBlank() ? watermarkFilter : "(" + combinedFilter + ") and (" + watermarkFilter + ")";
        List<Map<String, Object>> fresh = new ArrayList<>();
        Set<Version> seen = new HashSet<>();
        pollIncidents(runContext, combinedFilter, page ->
        {
            for (Map<String, Object> incident : page) {
                Version version = version(incident);
                if (version.modified().isBefore(lowerBound) || previous.boundary().contains(version) || !seen.add(version)) {
                    continue;
                }
                fresh.add(incident);
                if (fresh.size() == limit) {
                    return false;
                }
            }
            return true;
        });
        if (fresh.isEmpty()) {
            return Optional.empty();
        }
        Instant maximum = previous.timestamp();
        Set<Version> boundary = new HashSet<>(previous.boundary());
        for (Map<String, Object> incident : fresh) {
            Version version = version(incident);
            if (maximum == null || version.modified().isAfter(maximum)) {
                maximum = version.modified();
                boundary.clear();
            }
            if (version.modified().equals(maximum)) {
                boundary.add(version);
            }
        }
        Execution execution = TriggerService.generateExecution(this, conditionContext, context, Output.builder().incidents(fresh).count(fresh.size()).build());
        kv.put(key, new KVValueAndMetadata(new KVMetadata("Sentinel incident watermark", (Duration) null), JacksonMapper.ofJson().writeValueAsBytes(new Watermark(maximum, boundary))));
        return Optional.of(execution);
    }

    protected Instant now() {
        return Instant.now();
    }

    protected void pollIncidents(RunContext runContext, String combinedFilter, AbstractSentinel.PageConsumer consumer) throws Exception {
        incidentTask().incidentPages(runContext, combinedFilter, "properties/lastModifiedTimeUtc asc", consumer);
    }

    protected ListIncidents incidentTask() {
        return ListIncidents.builder()
            .id(id).type(ListIncidents.class.getName())
            .tenantId(tenantId).clientId(clientId).clientSecret(clientSecret).pemCertificate(pemCertificate)
            .subscriptionId(subscriptionId).resourceGroup(resourceGroup).workspaceName(workspaceName)
            .severities(severities).statuses(statuses).top(top).build();
    }

    static String stateKey(String flowId, String triggerId) {
        return "sentinel_watermark_" + flowId.length() + "_" + flowId + "_" + triggerId;
    }

    private static Version version(Map<String, Object> incident) {
        Object identity = incident.get("id");
        if (
            !(identity instanceof String name) || name.isBlank() || !(incident.get("properties") instanceof Map<?, ?> properties)
                || !(properties.get("lastModifiedTimeUtc") instanceof String modified)
        ) {
            throw new IllegalArgumentException("Sentinel incident is missing its id or lastModifiedTimeUtc");
        }
        return new Version(name, Instant.parse(modified), (String) incident.get("etag"));
    }

    record Version(String id, Instant modified, String etag) {
    }

    record Watermark(Instant timestamp, Set<Version> boundary) {
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "New or modified incidents")
        private final List<Map<String, Object>> incidents;
        @Schema(title = "Incident count")
        private final Integer count;
    }
}
