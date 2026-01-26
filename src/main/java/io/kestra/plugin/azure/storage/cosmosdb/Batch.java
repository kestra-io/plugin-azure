package io.kestra.plugin.azure.storage.cosmosdb;

import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosDiagnostics;
import com.azure.cosmos.models.*;
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

import java.time.Duration;
import java.util.List;
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
                id: azure_cosmos_container_batch_create_item
                namespace: company.team

                tasks:
                  - id: batch_create
                    type: io.kestra.plugin.azure.storage.cosmosdb.Batch
                    endpoint: "https://yourcosmosaccount.documents.azure.com"
                    databaseId: your_data_base_id
                    containerId: your_container_id
                    tenantId: "{{ secret('AZURE_TENANT_ID') }}"
                    clientId: "{{ secret('AZURE_CLIENT_ID') }}"
                    clientSecret: "{{ secret('AZURE_CLIENT_SECRET') }}"
                    items:
                        - id: item_one
                          key: value
                        - id: item_two
                          key: value
                """
        )
    }
)
@Schema(title = "Batch creates a new Cosmos item and returns its respective Cosmos batch response output.")
public class Batch extends AbstractCosmosContainerTask<Batch.BatchResponseOutput> implements RunnableTask<Batch.BatchResponseOutput> {
    @NotNull
    @Schema(
        name = "partitionKeyValue",
        title = "Partition key value",
        description = "Single partition key value shared by every item in the batch (e.g. \"US\" if your key is /country)."
    )
    Property<String> partitionKeyValue;

    @NotNull
    @Schema(
        name = "items",
        title = "Documents to create in one batch",
        description = "List of documents; each must include the partition key path matching partitionKeyValue."
    )
    Property<List<Map<String, Object>>> items;

    @Override
    protected BatchResponseOutput run(RunContext runContext, CosmosAsyncContainer cosmosContainer) throws IllegalVariableEvaluationException {
        String rPartitionKeyValue = runContext.render(partitionKeyValue).as(String.class).orElseThrow(
            () -> new IllegalVariableEvaluationException("partitionKeyValue cannot be null or empty")
        );

        List<Map<String, Object>> rItems = runContext.render(items).asList(Map.class);

        if (rItems.isEmpty()) {
            throw new IllegalVariableEvaluationException("items cannot be empty");
        }

        CosmosBatch batch = CosmosBatch.createCosmosBatch(
            new PartitionKey(rPartitionKeyValue)
        );

        rItems.forEach(batch::createItemOperation);

        return Batch.BatchResponseOutput.from(
            Objects.requireNonNull(cosmosContainer.executeCosmosBatch(batch).block())
        );
    }

    public record BatchResponseOutput(
        Map<String, String> responseHeaders,
        int statusCode,
        int subStatusCode,
        String errorMessage,
        List<CosmosBatchOperationResult> results,
        CosmosDiagnostics diagnostics,
        double requestCharge,
        String sessionToken,
        String activityId,
        Duration retryAfterDuration,
        int responseLength,
        Duration duration,
        boolean successStatusCode
    ) implements io.kestra.core.models.tasks.Output {
        public static BatchResponseOutput from(CosmosBatchResponse r) {
            return new BatchResponseOutput(
                r.getResponseHeaders(),
                r.getStatusCode(),
                r.getSubStatusCode(),
                r.getErrorMessage(),
                r.getResults(),
                r.getDiagnostics(),
                r.getRequestCharge(),
                r.getSessionToken(),
                r.getActivityId(),
                r.getRetryAfterDuration(),
                r.getResponseLength(),
                r.getDuration(),
                r.isSuccessStatusCode()
            );
        }
    }
}
