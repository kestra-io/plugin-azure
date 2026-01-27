package io.kestra.plugin.azure.storage.cosmosdb;

import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.FeedRange;
import com.azure.cosmos.models.PartitionKey;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;

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
                id: azure_storage_cosmos_queries
                namespace: company.team

                tasks:
                  - id: cosmos_queries
                    type: io.kestra.plugin.azure.storage.cosmosdb.Queries
                    endpoint: "https://yourcosmosaccount.documents.azure.com"
                    tenantId: "{{ secret('AZURE_TENANT_ID') }}"
                    clientId: "{{ secret('AZURE_CLIENT_ID') }}"
                    clientSecret: "{{ secret('AZURE_CLIENT_SECRET') }}"
                    queries:
                      query-one:
                        query: SELECT * FROM c
                      query-two:
                        query: SELECT * FROM c WHERE c.id = 'test'
                        partitionKeyDefinition:
                          paths: ["/id"]
                          kind: HASH
                          version: V2
                        partitionKey:
                          id: test
                """
        )
    }
)
@Schema(
    title = "Runs multiple queries on Cosmos items and returns their respective Cosmos query response outputs.",
    description = "Runs multiple labeled Cosmos SQL queries in one task; each result set is returned under its label."
)
public class Queries extends AbstractCosmosContainerTask<Queries.Output> implements RunnableTask<Queries.Output> {
    private static final Logger log = LoggerFactory.getLogger(Queries.class);

    @NotNull
    @Schema(
        title = "Named queries to execute",
        description = "Map of label to query options. Each entry runs independently and returns under its label."
    )
    private Property<Map<String, QueriesOptions>> queries;

    @Override
    protected Output run(RunContext runContext, CosmosAsyncContainer cosmosContainer) throws Exception {
        Map<String, QueriesOptions> rQueries = runContext.render(queries).asMap(String.class, QueriesOptions.class);

        Mono<Map<String, List<Map>>> results = Flux.fromIterable(rQueries.entrySet())
            .flatMap(entry ->
                Mono.fromCallable(() -> getRequestOptions(entry.getValue()))
                    .flatMap(opts -> cosmosContainer
                        .queryItems(entry.getValue().getQuery(), opts, Map.class)
                        .collectList()
                        .map(list -> Map.entry(entry.getKey(), list))
                    )
            )
            .collectMap(Map.Entry::getKey, Map.Entry::getValue);

        try {
            return new Output(results.block());
        } catch (Exception e) {
            log.error("Failed to run Queries: {}", rQueries, e);
            if (e.getCause() != null && e.getCause() instanceof Exception) {
                throw (Exception) e.getCause();
            }
            throw e;
        }
    }

    private static CosmosQueryRequestOptions getRequestOptions(QueriesOptions options) throws IllegalVariableEvaluationException {
        CosmosQueryRequestOptions queryRequestOptions = new CosmosQueryRequestOptions();

        boolean hasPartitionKeyDefinition = options.getPartitionKeyDefinition() != null;
        boolean hasPartitionKey = options.getPartitionKey() != null;
        boolean hasFeedRangePartitionKey = options.getFeedRangePartitionKey() != null;

        if (hasPartitionKeyDefinition && hasPartitionKey == hasFeedRangePartitionKey) {
            throw new IllegalVariableEvaluationException(
                "When partitionKeyDefinition is set, exactly one of partitionKey or feedRangePartitionKey must be set"
            );
        }

        if ((hasPartitionKey || hasFeedRangePartitionKey) && !hasPartitionKeyDefinition) {
            throw new IllegalVariableEvaluationException(
                "PartitionKeyDefinition must be set when partitionKey or feedRangePartitionKey is set"
            );
        }

        if (hasPartitionKeyDefinition && hasPartitionKey) {
            queryRequestOptions.setPartitionKey(PartitionKey.fromItem(
                options.partitionKey,
                options.partitionKeyDefinition.toAzurePartitionKeyDefinition())
            );
        }

        if (hasFeedRangePartitionKey && hasPartitionKeyDefinition) {
            queryRequestOptions.setFeedRange(FeedRange.forLogicalPartition(
                    PartitionKey.fromItem(
                        options.feedRangePartitionKey,
                        options.partitionKeyDefinition.toAzurePartitionKeyDefinition()
                    )
                )
            );
        }

        if (options.excludeRegions != null) {
            queryRequestOptions.setExcludedRegions(options.excludeRegions);
        }

        return queryRequestOptions;
    }

    @ToString
    @EqualsAndHashCode
    @Getter
    @SuperBuilder
    @NoArgsConstructor
    public static class QueriesOptions {
        @NotNull
        @Schema(
            title = "SQL query string",
            description = "Cosmos SQL text to execute for this entry."
        )
        private String query;

        @Schema(
            title = "Regions to exclude",
            description = """
                List of regions to be excluded for the request/retries. Example \"East US\" or \"East US, West US\" \
                These regions will be excluded from the preferred regions list. If all the regions are excluded, the \
                request will be sent to the primary region for the account. The primary region is the write region in a\
                 single master account and the hub region in a multi-master account.
            """
        )
        private List<String> excludeRegions;

        @Schema(
            title = "Partition key values",
            description = "Map of partition key path to value (e.g. `{ \"country\": \"US\" }`). Requires partitionKeyDefinition.",
            requiredProperties = "partitionKeyDefinition"
        )
        private Map<String, Object> partitionKey;

        @Schema(
            title = "Partition key definition (paths, kind, version)",
            description = "Schema of the container partition key used to build PartitionKey objects."
        )
        private PartitionKeyDefinition partitionKeyDefinition;

        @Schema(
            title = "Feed range partition key values",
            description = "Map of partition key path to value used to build a feed range; mutually exclusive with partitionKey.",
            requiredProperties = "partitionKeyDefinition"
        )
        private Map<String, Object> feedRangePartitionKey;
    }

    public record Output(
        Map<String, List<Map>> results
    ) implements io.kestra.core.models.tasks.Output { }
}
