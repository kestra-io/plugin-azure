package io.kestra.plugin.azure.storage.cosmosdb;

import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.models.*;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.List;
import java.util.Map;
import java.util.Optional;


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
                id: azure_storage_cosmos_query
                namespace: company.team

                tasks:
                  - id: bulk
                    type: io.kestra.plugin.azure.storage.cosmosdb.Query
                    endpoint: "https://yourstorageaccount.blob.core.windows.net"
                    databaseId: your_data_base_id
                    containerId: your_container_id
                    tenantId: "{{ secret('AZURE_TENANT_ID') }}"
                    clientId: "{{ secret('AZURE_CLIENT_ID') }}"
                    clientSecret: "{{ secret('AZURE_CLIENT_SECRET') }}"
                    query: "SELECT * FROM c"
                """
        )
    },
    metrics = {
        @Metric(name = "records.count", type = Counter.TYPE, description = "The total number of entities processed in the bulk operation.")
    }
)
@Schema(title = "Queries Cosmos items and returns its respective Cosmos query response output.")
public class Query extends AbstractCosmosContainerTask<Query.Output> implements RunnableTask<Query.Output> {
    @NotNull
    @Schema(title = "SQL query string")
    private Property<String> query;

    @Schema(
        title = "Regions to exclude",
        description = """
            List of regions to be excluded for the request/retries. Example \"East US\" or \"East US, \
            West US\" These regions will be excluded from the preferred regions list. If all the regions are excluded, \
            the request will be sent to the primary region for the account. The primary region is the write region in a \
            single master account and the hub region in a multi-master account.
            """
    )
    private Property<List<String>> excludeRegions;

    @Schema(
        title = "Partition key values",
        description = """
            Map of partition key path to value (e.g. `{ "country": "US" }` for a `/country` key). \
            Use with `partitionKeyDefinition` to target a logical partition.
            """,
        requiredProperties = "partitionKeyDefinition"
    )
    private Property<Map<String, Object>> partitionKey;

    @Schema(
        title = "Partition key definition (paths, kind, version)",
        description = """
            Defines the partition key schema (paths, kind, version). Required when using `partitionKey` \
            or `feedRangePartitionKey` so the task can build the correct `PartitionKey`.
            """
    )
    private Property<PartitionKeyDefinition> partitionKeyDefinition;

    @Schema(
        title = "Feed range partition key values",
        description = """
            Map of partition key path to value used to build a feed range (e.g. `{ "country": "US" }`). \
            Must be used with `partitionKeyDefinition`; mutually exclusive with `partitionKey`.
            """,
        requiredProperties = "partitionKeyDefinition"
    )
    private Property<Map<String, Object>> feedRangePartitionKey;

    @Override
    protected Output run(RunContext runContext, CosmosAsyncContainer cosmosContainer) throws IllegalVariableEvaluationException {
        String rQuery = runContext.render(query).as(String.class).orElseThrow(
            () -> new RuntimeException("Missing required query field")
        );

        List<String> rExcludeRegions = runContext.render(excludeRegions).asList(String.class);
        Map<String, Object> rPartitionKey = runContext.render(partitionKey).asMap(String.class, Object.class);
        Map<String, Object> rFeedRangePartitionKey = runContext.render(feedRangePartitionKey)
            .asMap(String.class, Object.class);

        Optional<PartitionKeyDefinition> rPartitionKeyDefinition = runContext
            .render(partitionKeyDefinition)
            .as(PartitionKeyDefinition.class);

        CosmosQueryRequestOptions options = new CosmosQueryRequestOptions();

        if (!rExcludeRegions.isEmpty()) {
            options.setExcludedRegions(rExcludeRegions);
        }

        boolean hasPartitionKeyDefinition = rPartitionKeyDefinition.isPresent();
        boolean hasPartitionKey = !rPartitionKey.isEmpty();
        boolean hasFeedRangePartitionKey = !rFeedRangePartitionKey.isEmpty();

        if (hasPartitionKeyDefinition && hasPartitionKey == hasFeedRangePartitionKey) {
            throw new IllegalVariableEvaluationException("When partitionKeyDefinition is set, exactly one of partitionKey or feedRangePartitionKey must be set");
        }

        if ((hasPartitionKey || hasFeedRangePartitionKey) && !hasPartitionKeyDefinition) {
            throw new IllegalVariableEvaluationException("PartitionKeyDefinition must be set when partitionKey or feedRangePartitionKey is set");
        }

        if (rPartitionKeyDefinition.isPresent() && !rPartitionKey.isEmpty()) {
            options.setPartitionKey(
                PartitionKey.fromItem(rPartitionKey, rPartitionKeyDefinition.get().toAzurePartitionKeyDefinition())
            );
        }

        rPartitionKeyDefinition.ifPresent(partitionKeyDefinition -> {
            if (!rPartitionKey.isEmpty()) {
                options.setPartitionKey(
                    PartitionKey.fromItem(rPartitionKey, partitionKeyDefinition.toAzurePartitionKeyDefinition())
                );
            }

            if (!rFeedRangePartitionKey.isEmpty()) {
                options.setFeedRange(FeedRange.forLogicalPartition(
                    PartitionKey.fromItem(rFeedRangePartitionKey, partitionKeyDefinition.toAzurePartitionKeyDefinition())
                ));
            }

        });

        return new Output(
            cosmosContainer.queryItems(rQuery, options, Map.class).byPage()
                .flatMapIterable(FeedResponse::getResults)
                .collectList()
                .block()
        );
    }

    public record Output(
        List<Map> queryResults
    ) implements io.kestra.core.models.tasks.Output {}
}
