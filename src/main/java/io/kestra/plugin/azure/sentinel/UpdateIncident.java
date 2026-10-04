package io.kestra.plugin.azure.sentinel;

import java.util.*;

import io.kestra.core.models.annotations.*;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(title = "Update a Sentinel incident", description = "Reads and merges the writable incident properties, preserving omitted values and its ETag for optimistic concurrency.")
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
    protected Property<String> incidentId;

    @Schema(title = "Incident status")
    protected Property<String> status;

    @Schema(title = "Incident severity")
    protected Property<String> severity;

    @Schema(title = "Closure classification")
    protected Property<String> classification;

    @Schema(title = "Closure classification reason")
    protected Property<String> classificationReason;

    @Schema(title = "Closure classification comment")
    protected Property<String> classificationComment;

    @Schema(title = "Owner email")
    protected Property<String> ownerEmail;

    @Schema(title = "Owner object ID")
    protected Property<String> ownerObjectId;

    @Schema(title = "Incident title")
    protected Property<String> title;

    @Schema(title = "Incident description")
    protected Property<String> incidentDescription;

    @Schema(title = "User labels", description = "Replace user labels with these names, preserving system labels. An empty list clears user labels; omitted leaves labels unchanged.")
    protected Property<List<String>> labels;

    private static final Set<String> WRITABLE = Set.of(
        "title", "description", "severity", "status", "classification",
        "classificationReason", "classificationComment", "firstActivityTimeUtc", "lastActivityTimeUtc", "owner", "labels"
    );

    @Override
    public Output run(RunContext context) throws Exception {
        String id = required(context, incidentId, "incidentId");
        Map<String, Object> changes = new LinkedHashMap<>();
        put(context, changes, "status", status);
        put(context, changes, "severity", severity);
        put(context, changes, "classification", classification);
        put(context, changes, "classificationReason", classificationReason);
        put(context, changes, "classificationComment", classificationComment);
        put(context, changes, "title", title);
        put(context, changes, "description", incidentDescription);
        Map<String, Object> ownerChanges = new LinkedHashMap<>();
        put(context, ownerChanges, "email", ownerEmail);
        put(context, ownerChanges, "objectId", ownerObjectId);
        List<String> renderedLabels = labels == null ? null : context.render(labels).asList(String.class);
        if (renderedLabels != null && renderedLabels.stream().anyMatch(s -> s == null || s.isBlank())) {
            throw new IllegalArgumentException("labels must contain nonempty names");
        }
        var uri = incidentsUri(context, "/" + encode(id), Map.of());
        Map<String, Object> existing = armRequest(context, "GET", uri, null);
        return new Output(armRequest(context, "PUT", uri, merge(existing, changes, ownerChanges, renderedLabels)));
    }

    private static void put(RunContext context, Map<String, Object> target, String key, Property<String> property) throws Exception {
        context.render(property).as(String.class).ifPresent(value -> target.put(key, value));
    }

    static Map<String, Object> merge(Map<String, Object> existing, Map<String, Object> changes,
        Map<String, Object> ownerChanges, List<String> labels) {
        if (!(existing.get("properties") instanceof Map<?, ?> old) || !(existing.get("etag") instanceof String etag) || etag.isBlank()) {
            throw new IllegalStateException("Azure Sentinel incident response is missing properties or etag");
        }
        Map<String, Object> properties = new LinkedHashMap<>();
        WRITABLE.forEach(key ->
        {
            if (old.containsKey(key)) {
                properties.put(key, old.get(key));
            }
        });
        properties.putAll(changes);
        if (!ownerChanges.isEmpty()) {
            Map<String, Object> owner = new LinkedHashMap<>();
            if (properties.get("owner") instanceof Map<?, ?> previous) {
                previous.forEach((key, value) -> owner.put((String) key, value));
            }
            owner.putAll(ownerChanges);
            properties.put("owner", owner);
        }
        if (labels != null) {
            List<Map<String, Object>> merged = new ArrayList<>();
            if (properties.get("labels") instanceof List<?> previous) {
                for (Object value : previous) {
                    if (value instanceof Map<?, ?> label && "System".equals(label.get("labelType"))) {
                        Map<String, Object> copy = new LinkedHashMap<>();
                        label.forEach((key, item) -> copy.put((String) key, item));
                        merged.add(copy);
                    }
                }
            }
            labels.stream().distinct().forEach(name -> merged.add(Map.of("labelName", name, "labelType", "User")));
            properties.put("labels", merged);
        }
        for (String name : List.of("title", "severity", "status")) {
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
