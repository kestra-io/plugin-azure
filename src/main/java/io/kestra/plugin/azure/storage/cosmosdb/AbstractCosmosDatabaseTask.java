package io.kestra.plugin.azure.storage.cosmosdb;

import com.azure.cosmos.*;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Output;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.AbstractAzureIdentityConnection;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.StringUtils;

import java.util.Optional;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractCosmosDatabaseTask<T extends Output> extends AbstractAzureIdentityConnection {
    @Schema(
        title = "Choose request consistency",
        description = "Consistency level sent to Cosmos; defaults to SESSION."
    )
    @Builder.Default()
    private Property<ConsistencyLevel> consistencyLevel = Property.ofValue(ConsistencyLevel.SESSION);

    @Schema(
        title = "Cosmos account endpoint",
        description = "Base account URL (e.g. https://<account>.documents.azure.com). Required when connectionString is absent."
    )
    private Property<String> endpoint;

    @Schema(
        title = "Return payload on writes",
        description = "When true (default), write responses include the document body; set false to reduce payload size."
    )
    @Builder.Default()
    private Property<Boolean> contentResponseOnWriteEnabled = Property.ofValue(DEFAULT_CONTENT_RESPONSE_ON_WRITE_ENABLED);

    @Schema(
        title = "Database ID",
        description = "Target database name inside the account; required."
    )
    @NotNull
    private Property<String> databaseId;

    @Schema(
        title = "Cosmos connection string",
        description = "Full connection string from Azure portal; overrides endpoint and Azure AD credentials when set."
    )
    protected Property<String> connectionString;

    private static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.SESSION;
    private static final boolean DEFAULT_CONTENT_RESPONSE_ON_WRITE_ENABLED = true;

    public T run(RunContext runContext) throws Exception {

        String rDataBaseId = runContext.render(databaseId).as(String.class)
            .orElseThrow(() -> new IllegalVariableEvaluationException("database id needed"));

        CosmosAsyncClient client = getClient(runContext);
        try (client) {
            return run(runContext, client.getDatabase(rDataBaseId));
        }
    }

    protected abstract T run(RunContext runContext, CosmosAsyncDatabase cosmosDatabase) throws Exception;

    private CosmosAsyncClient getClient(RunContext runContext) throws Exception {
        Optional<String> rConnectionString = runContext.render(this.connectionString).as(String.class);

        ConsistencyLevel rConsistencyLevel = runContext.render(this.consistencyLevel)
            .as(ConsistencyLevel.class)
            .orElse(DEFAULT_CONSISTENCY_LEVEL);

        boolean rContentResponseOnWriteEnabled = runContext.render(contentResponseOnWriteEnabled).as(Boolean.class)
            .orElse(DEFAULT_CONTENT_RESPONSE_ON_WRITE_ENABLED);


        CosmosClientBuilder cosmosClientBuilder = new CosmosClientBuilder()
            .consistencyLevel(rConsistencyLevel)
            .contentResponseOnWriteEnabled(rContentResponseOnWriteEnabled);

        if (rConnectionString.isPresent()) {
            cosmosClientBuilder
                .endpoint(StringUtils.substringBetween(rConnectionString.get(), "AccountEndpoint=", ";"))
                .key(StringUtils.substringBetween(rConnectionString.get(), "AccountKey=", ";"));
        } else {
            String rEndPoint = runContext.render(this.endpoint).as(String.class)
                .orElseThrow(() -> new Exception("Endpoint or ConnectionString needed"));
            cosmosClientBuilder.endpoint(rEndPoint);
            cosmosClientBuilder.credential(credentials(runContext));
        }

        return cosmosClientBuilder.buildAsyncClient();
    }
}
