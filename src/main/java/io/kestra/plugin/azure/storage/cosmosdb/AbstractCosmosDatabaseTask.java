package io.kestra.plugin.azure.storage.cosmosdb;

import com.azure.cosmos.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Output;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.azure.AbstractAzureIdentityConnection;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractCosmosDatabaseTask<T extends Output> extends AbstractAzureIdentityConnection implements RunnableTask<T> {
    @Schema(
        title = "consistencyLevel",
        description = "Represents the consistency levels supported for Azure Cosmos DB client operations in the Azure " +
            "Cosmos DB service."
    )
    @Builder.Default()
    private Property<ConsistencyLevel> consistencyLevel = Property.ofValue(ConsistencyLevel.SESSION);

    @Schema(title = "endpoint")
    private Property<String> endpoint;

    @Schema(
        title = "contentResponseOnWriteEnabled",
        description = "Sets the boolean to only return the headers and status code in Cosmos DB response in case of " +
            "Create, Update and Delete operations on CosmosItem.\n" +
        "If set to false (which is by default), service doesn't return payload in the response. It reduces networking " +
            "and CPU load by not sending the payload back over the network and serializing it on the client."
    )
    @Builder.Default()
    private Property<Boolean> contentResponseOnWriteEnabled = Property.ofValue(DEFAULT_CONTENT_RESPONSE_ON_WRITE_ENABLED);

    @Schema(title = "database ID")
    @NotNull
    private Property<String> databaseId;

    private static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.SESSION;
    private static final boolean DEFAULT_CONTENT_RESPONSE_ON_WRITE_ENABLED = true;

    protected final static ObjectMapper mapper = JacksonMapper.ofJson(false);

    @Override
    public T run(RunContext runContext) throws Exception {

        String rDataBaseId = runContext.render(databaseId).as(String.class)
            .orElseThrow(() -> new Exception("Database name needed"));

        final CosmosClient client = getClient(runContext);
        try (client) {
            return run(runContext, client.getDatabase(rDataBaseId));
        }
    }

    protected abstract T run(RunContext runContext, CosmosDatabase cosmosDatabase) throws IllegalVariableEvaluationException;

    private CosmosClient getClient(RunContext runContext) throws IllegalVariableEvaluationException {
        String rEndPoint = runContext.render(this.endpoint).as(String.class)
            .orElseThrow(() -> new RuntimeException("endPoint required"));

        ConsistencyLevel rConsistencyLevel = runContext.render(this.consistencyLevel)
            .as(ConsistencyLevel.class)
            .orElseThrow(() -> new RuntimeException("Consistency Level Reqired"));

        boolean rContentResponseOnWriteEnabled = runContext.render(contentResponseOnWriteEnabled).as(Boolean.class)
            .orElse(DEFAULT_CONTENT_RESPONSE_ON_WRITE_ENABLED);

        return new CosmosClientBuilder()
            .endpoint(rEndPoint)
            .credential(credentials(runContext))
            .consistencyLevel(rConsistencyLevel)
            .contentResponseOnWriteEnabled(rContentResponseOnWriteEnabled)
            .buildClient();
    }
}
