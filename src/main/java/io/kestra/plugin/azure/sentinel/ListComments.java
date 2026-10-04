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
@Schema(title = "List Sentinel incident comments", description = "Lists every page of comments attached to a Sentinel incident.")
@Plugin(examples = @Example(title = "List Sentinel incident comments", full = true, code = """
    id: sentinel_listcomments
    namespace: company.security

    inputs:
      - id: incidentId
        type: STRING

    tasks:
      - id: sentinel
        type: io.kestra.plugin.azure.sentinel.ListComments
        tenantId: "{{ secret('AZURE_TENANT_ID') }}"
        clientId: "{{ secret('AZURE_CLIENT_ID') }}"
        clientSecret: "{{ secret('AZURE_CLIENT_SECRET') }}"
        subscriptionId: "{{ secret('AZURE_SUBSCRIPTION_ID') }}"
        resourceGroup: rg-security
        workspaceName: law-sentinel
        incidentId: "{{ inputs.incidentId }}"
    """))
public class ListComments extends AbstractSentinel implements RunnableTask<ListComments.Output> {
    @NotNull
    @Schema(title = "Incident ID")
    protected Property<String> incidentId;

    @Override
    public Output run(RunContext context) throws Exception {
        List<Map<String, Object>> comments = new ArrayList<>();
        pages(context, incidentsUri(context, "/" + encode(required(context, incidentId, "incidentId")) + "/comments", Map.of()), page ->
        {
            comments.addAll(page);
            return true;
        });
        return new Output(comments, (long) comments.size());
    }

    public record Output(
        @Schema(title = "Incident comments") List<Map<String, Object>> comments,
        @Schema(title = "Number of comments") Long count) implements io.kestra.core.models.tasks.Output {
    }
}
