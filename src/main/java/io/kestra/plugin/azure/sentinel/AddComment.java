package io.kestra.plugin.azure.sentinel;

import java.util.Map;
import java.util.UUID;

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
    title = "Add a Sentinel incident comment",
    description = """
        Creates a comment with a generated UUID. Task retries can create additional comments."""
)
@Plugin(examples = @Example(title = "Add a Sentinel incident comment", full = true, code = """
    id: sentinel_addcomment
    namespace: company.security

    inputs:
      - id: incidentId
        type: STRING

    tasks:
      - id: sentinel
        type: io.kestra.plugin.azure.sentinel.AddComment
        tenantId: "{{ secret('AZURE_TENANT_ID') }}"
        clientId: "{{ secret('AZURE_CLIENT_ID') }}"
        clientSecret: "{{ secret('AZURE_CLIENT_SECRET') }}"
        subscriptionId: "{{ secret('AZURE_SUBSCRIPTION_ID') }}"
        resourceGroup: rg-security
        workspaceName: law-sentinel
        incidentId: "{{ inputs.incidentId }}"
        message: "Investigating from Kestra execution {{ execution.id }}"
    """))
public class AddComment extends AbstractSentinel implements RunnableTask<AddComment.Output> {
    @NotNull
    @Schema(title = "Incident ID")
    @PluginProperty(group = "main")
    protected Property<String> incidentId;

    @NotNull
    @Schema(title = "Comment message")
    @PluginProperty(group = "main")
    protected Property<String> message;

    @Override
    public Output run(RunContext context) throws Exception {
        var rIncidentId = required(context, incidentId, "incidentId");
        var rMessage = required(context, message, "message");
        var commentId = UUID.randomUUID().toString();
        var response = armRequest(
            context, "PUT",
            incidentsUri(context, "/" + encode(rIncidentId) + "/comments/" + commentId, Map.of()),
            Map.of("properties", Map.of("message", rMessage))
        );
        if (!(response.get("properties") instanceof Map<?, ?> properties) || !(properties.get("createdTimeUtc") instanceof String created)) {
            throw new IllegalStateException("Azure Sentinel comment response is missing createdTimeUtc");
        }
        return new Output(commentId, created);
    }

    public record Output(
        @Schema(title = "Created comment ID") String commentId,
        @Schema(
            title = "Comment creation time",
            description = """
                UTC timestamp returned by Azure."""
        ) String createdTimeUtc) implements io.kestra.core.models.tasks.Output {
    }
}
