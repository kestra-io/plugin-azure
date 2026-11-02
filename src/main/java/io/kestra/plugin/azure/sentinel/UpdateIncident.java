package io.kestra.plugin.azure.sentinel;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
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
    title = "Update a Sentinel incident",
    description = """
        Reads and merges the writable incident properties, preserving omitted values and its ETag for optimistic concurrency."""
)
@Plugin(examples = @Example(title = "Update a Sentinel incident", full = true, code = """
    id: sentinel_updateincident
    namespace: company.security

    inputs:
      - id: incidentId
        type: STRING

    tasks:
      - id: sentinel
        type: io.kestra.plugin.azure.sentinel.UpdateIncident
        tenantId: "{{ secret('AZURE_TENANT_ID') }}"
        clientId: "{{ secret('AZURE_CLIENT_ID') }}"
        clientSecret: "{{ secret('AZURE_CLIENT_SECRET') }}"
        subscriptionId: "{{ secret('AZURE_SUBSCRIPTION_ID') }}"
        resourceGroup: rg-security
        workspaceName: law-sentinel
        incidentId: "{{ inputs.incidentId }}"
        status: Active
    """))
public class UpdateIncident extends AbstractSentinel implements RunnableTask<UpdateIncident.Output> {
    @NotNull
    @Schema(title = "Incident ID")
    @PluginProperty(group = "main")
    protected Property<String> incidentId;

    @Schema(
        title = "Incident status",
        description = """
            New status of the incident. One of `New`, `Active` or `Closed`."""
    )
    @PluginProperty(group = "main")
    protected Property<IncidentStatus> status;

    @Schema(
        title = "Incident severity",
        description = """
            New severity of the incident. One of `High`, `Medium`, `Low` or `Informational`."""
    )
    @PluginProperty(group = "main")
    protected Property<IncidentSeverity> severity;

    @Schema(
        title = "Closure classification",
        description = """
            Why the incident was closed. One of `Undetermined`, `TruePositive`, `BenignPositive` or `FalsePositive`."""
    )
    @PluginProperty(group = "main")
    protected Property<IncidentClassification> classification;

    @Schema(
        title = "Closure classification reason",
        description = """
            Reason for the closure classification. One of `SuspiciousActivity`, `SuspiciousButExpected`,
            `IncorrectAlertLogic` or `InaccurateData`."""
    )
    @PluginProperty(group = "main")
    protected Property<IncidentClassificationReason> classificationReason;

    @Schema(title = "Closure classification comment")
    @PluginProperty(group = "main")
    protected Property<String> classificationComment;

    @Schema(
        title = "Owner email",
        description = """
            Replaces the owner with the supplied owner fields. Use ownerObjectId for the most reliable assignment."""
    )
    @PluginProperty(group = "main")
    protected Property<String> ownerEmail;

    @Schema(
        title = "Owner object ID",
        description = """
            Most reliable way to assign an incident. Replaces the owner with the supplied owner fields."""
    )
    @PluginProperty(group = "main")
    protected Property<String> ownerObjectId;

    @Schema(title = "Incident title")
    @PluginProperty(group = "main")
    protected Property<String> title;

    @Schema(title = "Incident description")
    @PluginProperty(group = "main")
    protected Property<String> incidentDescription;

    @Schema(
        title = "User labels",
        description = """
            Replace user labels with these names, preserving system labels.
            An empty list clears user labels; omitted leaves labels unchanged."""
    )
    @PluginProperty(group = "main")
    protected Property<List<String>> labels;

    private static final Set<String> WRITABLE = Set.of(
        "title", "description", "severity", "status", "classification",
        "classificationReason", "classificationComment", "firstActivityTimeUtc", "lastActivityTimeUtc", "owner", "labels"
    );

    @Override
    public Output run(RunContext context) throws Exception {
        var rIncidentId = required(context, incidentId, "incidentId");
        var changes = new LinkedHashMap<String, Object>();
        put(context, changes, "status", status, IncidentStatus.class);
        put(context, changes, "severity", severity, IncidentSeverity.class);
        put(context, changes, "classification", classification, IncidentClassification.class);
        put(context, changes, "classificationReason", classificationReason, IncidentClassificationReason.class);
        put(context, changes, "classificationComment", classificationComment, String.class);
        put(context, changes, "title", title, String.class);
        put(context, changes, "description", incidentDescription, String.class);
        var ownerChanges = new LinkedHashMap<String, Object>();
        put(context, ownerChanges, "email", ownerEmail, String.class);
        put(context, ownerChanges, "objectId", ownerObjectId, String.class);
        var rLabels = labels == null ? null : context.render(labels).asList(String.class);
        if (rLabels != null && rLabels.stream().anyMatch(s -> s == null || s.isBlank())) {
            throw new IllegalArgumentException("labels must contain nonempty names");
        }
        var uri = incidentsUri(context, "/" + encode(rIncidentId), Map.of());
        var existing = armRequest(context, "GET", uri, null);
        return new Output(armRequest(context, "PUT", uri, merge(existing, changes, ownerChanges, rLabels)));
    }

    private static <T> void put(RunContext context, Map<String, Object> target, String key, Property<T> property, Class<T> type) throws Exception {
        context.render(property).as(type).ifPresent(value -> target.put(key, value instanceof Enum<?> constant ? constant.name() : value));
    }

    static Map<String, Object> merge(Map<String, Object> existing, Map<String, Object> changes,
        Map<String, Object> ownerChanges, List<String> labels) {
        if (!(existing.get("properties") instanceof Map<?, ?> old) || !(existing.get("etag") instanceof String etag) || etag.isBlank()) {
            throw new IllegalStateException("Azure Sentinel incident response is missing properties or etag");
        }
        var properties = new LinkedHashMap<String, Object>();
        WRITABLE.forEach(key ->
        {
            if (old.containsKey(key)) {
                properties.put(key, old.get(key));
            }
        });
        properties.putAll(changes);
        if (!ownerChanges.isEmpty()) {
            properties.put("owner", new LinkedHashMap<>(ownerChanges));
        }
        if (labels != null) {
            var merged = new ArrayList<Map<String, Object>>();
            if (properties.get("labels") instanceof List<?> previous) {
                for (Object value : previous) {
                    if (value instanceof Map<?, ?> label && "System".equals(label.get("labelType"))) {
                        var copy = new LinkedHashMap<String, Object>();
                        label.forEach((key, item) -> copy.put((String) key, item));
                        merged.add(copy);
                    }
                }
            }
            labels.stream().distinct().forEach(name -> merged.add(Map.of("labelName", name, "labelType", "User")));
            properties.put("labels", merged);
        }
        for (var name : List.of("title", "severity", "status")) {
            if (!(properties.get(name) instanceof String text) || text.isBlank()) {
                throw new IllegalArgumentException("Incident " + name + " must not be empty");
            }
        }
        return Map.of("etag", etag, "properties", properties);
    }

    public record Output(@Schema(title = "Updated incident resource") Map<String, Object> incident)
        implements
            io.kestra.core.models.tasks.Output {
    }

}
