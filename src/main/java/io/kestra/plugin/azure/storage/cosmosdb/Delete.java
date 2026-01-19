package io.kestra.plugin.azure.storage.cosmosdb;

import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosDiagnostics;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: azure_cosmos_container_delete_item
                namespace: company.team

                tasks:
                  - id: delete
                    type: io.kestra.plugin.azure.storage.cosmosdb.Delete
                    endpoint: "https://yourcosmosaccount.documents.azure.com"
                    databaseId: your_data_base_id
                    containerId: your_container_id
                    tenantId: "{{ secret('AZURE_TENANT_ID') }}"
                    clientId: "{{ secret('AZURE_CLIENT_ID') }}"
                    clientSecret: "{{ secret('AZURE_CLIENT_SECRET') }}"
                    item:
                        id: item_id
                """
        )
    }
)
@Schema(title = "Deletes a Cosmos item and returns its respective Cosmos item response.")
public class Delete extends AbstractCosmosContainerTask<Delete.Output> implements RunnableTask<Delete.Output> {
    @NotNull
    @Schema(title = "item")
    private Property<Map<String, Object>> item;


    @Override
    protected Output run(RunContext runContext, CosmosAsyncContainer cosmosContainer) throws IllegalVariableEvaluationException {
        Map<String, Object> rItem = runContext.render(item).asMap(String.class, Object.class);

        return Output.from(
            Objects.requireNonNull(cosmosContainer.deleteItem(rItem, null).block())
        );
    }

    public record Output(
        Object item,
        String maxResourceQuota,
        String currentResourceQuotaUsage,
        String activityId,
        double requestCharge,
        int statusCode,
        String sessionToken,
        Map<String, String> responseHeaders,
        CosmosDiagnostics diagnostics,
        Duration duration,
        String eTag
    ) implements io.kestra.core.models.tasks.Output {
        public static Output from(com.azure.cosmos.models.CosmosItemResponse<Object> r) {
            return new Output(
                r.getItem(),
                r.getMaxResourceQuota(),
                r.getCurrentResourceQuotaUsage(),
                r.getActivityId(),
                r.getRequestCharge(),
                r.getStatusCode(),
                r.getSessionToken(),
                r.getResponseHeaders(),
                r.getDiagnostics(),
                r.getDuration(),
                r.getETag()
            );
        }
    }
}
