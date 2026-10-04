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
@Schema(title = "Get a Sentinel incident", description = "Retrieves one incident, optionally including its associated alerts.")
@Plugin(examples = @Example(title = "Get a Sentinel incident", full = true, code = """
    id: sentinel_getincident
    namespace: company.security

    inputs:
      - id: incidentId
        type: STRING

    tasks:
      - id: sentinel
        type: io.kestra.plugin.azure.sentinel.GetIncident
        tenantId: "{{ secret('AZURE_TENANT_ID') }}"
        clientId: "{{ secret('AZURE_CLIENT_ID') }}"
        clientSecret: "{{ secret('AZURE_CLIENT_SECRET') }}"
        subscriptionId: "{{ secret('AZURE_SUBSCRIPTION_ID') }}"
        resourceGroup: rg-security
        workspaceName: law-sentinel
        incidentId: "{{ inputs.incidentId }}"
    """))
public class GetIncident extends AbstractSentinel implements RunnableTask<GetIncident.Output> {
    @NotNull
    @Schema(title = "Incident ID")
    protected Property<String> incidentId;

    @Builder.Default
    @Schema(title = "Include incident alerts", description = "Fetch the associated alerts with an additional request. Defaults to false.")
    protected Property<Boolean> includeAlerts = Property.ofValue(false);

    @Override
    public Output run(RunContext context) throws Exception {
        String suffix = "/" + encode(required(context, incidentId, "incidentId"));
        Map<String, Object> incident = armRequest(context, "GET", incidentsUri(context, suffix, Map.of()), null);
        List<Map<String, Object>> alerts = null;
        if (context.render(includeAlerts).as(Boolean.class).orElse(false)) {
            alerts = values(armRequest(context, "POST", incidentsUri(context, suffix + "/alerts", Map.of()), null));
        }
        return new Output(incident, alerts);
    }

    public record Output(
        @Schema(title = "Incident resource") Map<String, Object> incident,
        @Schema(title = "Associated alerts", description = "Only populated when includeAlerts is true.") List<Map<String, Object>> alerts) implements io.kestra.core.models.tasks.Output {
    }
}
