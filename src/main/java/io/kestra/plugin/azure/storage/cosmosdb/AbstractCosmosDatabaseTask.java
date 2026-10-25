package io.kestra.plugin.azure.storage.cosmosdb;

import java.net.URI;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;

import com.azure.cosmos.*;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Output;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.shared.AbstractAzureIdentityConnection;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

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
    @PluginProperty(group = "advanced")
    private Property<ConsistencyLevel> consistencyLevel = Property.ofValue(ConsistencyLevel.SESSION);

    @Schema(
        title = "Cosmos account endpoint",
        description = "Base account URL (e.g. https://<account>.documents.azure.com). Required when connectionString is absent. A `localhost` or `127.0.0.1` endpoint, such as a Cosmos emulator, is reached in gateway mode."
    )
    @PluginProperty(group = "connection")
    private Property<String> endpoint;

    @Schema(
        title = "Return payload on writes",
        description = "When true (default), write responses include the document body; set false to reduce payload size."
    )
    @Builder.Default()
    @PluginProperty(group = "advanced")
    private Property<Boolean> contentResponseOnWriteEnabled = Property.ofValue(DEFAULT_CONTENT_RESPONSE_ON_WRITE_ENABLED);

    @Schema(
        title = "Database ID",
        description = "Target database name inside the account; required."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> databaseId;

    @Schema(
        title = "Cosmos connection string",
        description = "Full connection string from Azure portal; overrides endpoint and Azure AD credentials when set."
    )
    @PluginProperty(secret = true, group = "connection")
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
            var rEndpoint = StringUtils.substringBetween(rConnectionString.get(), "AccountEndpoint=", ";");
            cosmosClientBuilder
                .endpoint(rEndpoint)
                .key(StringUtils.substringBetween(rConnectionString.get(), "AccountKey=", ";"));
            useGatewayModeForLocalEmulator(cosmosClientBuilder, rEndpoint);
        } else {
            var rEndpoint = runContext.render(this.endpoint).as(String.class)
                .orElseThrow(() -> new Exception("Endpoint or ConnectionString needed"));
            cosmosClientBuilder.endpoint(rEndpoint);
            cosmosClientBuilder.credential(credentials(runContext));
            useGatewayModeForLocalEmulator(cosmosClientBuilder, rEndpoint);
        }

        return cosmosClientBuilder.buildAsyncClient();
    }

    // Local emulators (Floci, the Cosmos emulator) only serve the gateway, direct mode dials replica ports they do not expose.
    private static void useGatewayModeForLocalEmulator(CosmosClientBuilder builder, String rEndpoint) {
        if (rEndpoint == null) {
            return;
        }
        try {
            var host = URI.create(rEndpoint).getHost();
            if ("localhost".equalsIgnoreCase(host) || "127.0.0.1".equals(host)) {
                builder.gatewayMode().endpointDiscoveryEnabled(false);
            }
        } catch (IllegalArgumentException e) {
            // the Cosmos client reports a malformed endpoint itself
        }
    }
}
