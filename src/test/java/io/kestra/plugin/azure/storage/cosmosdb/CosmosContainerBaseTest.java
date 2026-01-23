package io.kestra.plugin.azure.storage.cosmosdb;

import com.azure.cosmos.models.PartitionKeyDefinitionVersion;
import com.azure.cosmos.models.PartitionKind;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import lombok.SneakyThrows;
import org.assertj.core.api.AbstractThrowableAssert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@KestraTest(startRunner = true, environments = "sp")
@EnabledIfEnvironmentVariable(named = "AZURE_CONNECTION_STRING", matches = ".+")
public abstract class CosmosContainerBaseTest<T extends AbstractCosmosContainerTask.AbstractCosmosContainerTaskBuilder<?,?,?>> {
    @Value("${kestra.variables.globals.azure.cosmos.connection-string}")
    protected String connectionString;

    @Value("${kestra.variables.globals.azure.cosmos.endpoint}")
    protected String endpoint;

    @Value("${kestra.variables.globals.azure.cosmos.container-id}")
    protected String containerId;

    @Value("${kestra.variables.globals.azure.cosmos.database-id}")
    protected String databaseId;

    @Inject
    protected RunContextFactory runContextFactory = new RunContextFactory();

    protected static final PartitionKeyDefinition PARTITION_KEY_DEFINITION = new PartitionKeyDefinition(
        List.of("/pk"),
        PartitionKind.HASH,
        PartitionKeyDefinitionVersion.V2
    );


    private static final Logger log = LoggerFactory.getLogger(CosmosContainerBaseTest.class);

    private final List<Map<String, Object>> createdItemsToRemove = new ArrayList<>();

    protected final String testId = IdUtils.create();

    @SneakyThrows
    @AfterAll
    void tearDown() {
        createdItemsToRemove.forEach(this::deleteItem);
    }

    @Test
    void shouldThrowErrorWhenDatabaseIdNotSet() {
        //region GIVEN
        AbstractCosmosContainerTask.AbstractCosmosDatabaseTaskBuilder<?,?,?> cosmosContainerTaskBuilder =
            this.getBaseTaskBuilder().databaseId(Property.ofValue(null));

        //endregion

        //region WHEN
        final RunContext runContext = runContextFactory.of();

        AbstractThrowableAssert<?, ?> throwableAssert = assertThatThrownBy(() -> cosmosContainerTaskBuilder.build().run(runContext));
        //endregion

        //region WHEN
        throwableAssert.isInstanceOf(IllegalVariableEvaluationException.class);
        throwableAssert.hasMessageContaining("database id needed");
        //endregion
    }

    @Test
    void shouldThrowErrorWhenContainerIdNotSet() {
        //region GIVEN
        AbstractCosmosContainerTask.AbstractCosmosDatabaseTaskBuilder<?,?,?> cosmosContainerTaskBuilder =
            this.getBaseTaskBuilder().containerId(Property.ofValue(null));

        //endregion

        //region WHEN
        final RunContext runContext = runContextFactory.of();

        AbstractThrowableAssert<?, ?> throwableAssert = assertThatThrownBy(() -> cosmosContainerTaskBuilder.build().run(runContext));
        //endregion

        //region WHEN
        throwableAssert.isInstanceOf(IllegalVariableEvaluationException.class);
        throwableAssert.hasMessageContaining("container id needed");
        //endregion
    }


    protected abstract T instantiateBaseTaskBuilder();

    protected T getBaseTaskBuilder() {
        return applyAuth(instantiateBaseTaskBuilder()
            .id(this.getClass().getSimpleName())
            .databaseId(Property.ofValue(databaseId))
            .containerId(Property.ofValue(containerId))
            .endpoint(Property.ofValue(endpoint))
        );
    }

    @SuppressWarnings("unchecked")
    protected T applyAuth(AbstractCosmosContainerTask.AbstractCosmosContainerTaskBuilder<?,?,?> containerTask) {
        return (T) containerTask.connectionString(Property.ofValue(connectionString));
    }

    protected Map<String, Object> createItem(String id, Map<String, Object> item) throws Exception {
        return createItem(id, item, true);
    }

    protected Map<String, Object> createItem(String id, Map<String, Object> item, boolean autoRemove) throws Exception {
        Map<String, Object> itemWithId = new HashMap<>(item);
        String itemIdWithTestId = id + "_" + testId;
        itemWithId.put("id", itemIdWithTestId);
        itemWithId.putIfAbsent("pk", itemIdWithTestId);
        if (autoRemove) {
            createdItemsToRemove.add(itemWithId);
        }

        return cosmosContainerTaskBuilder(CreateItem.builder())
            .item(Property.ofValue(itemWithId))
            .build()
            .run(runContextFactory.of()).item();
    }

    protected void deleteItem(Map<String, Object> item) {
        try {
            cosmosContainerTaskBuilder(Delete.builder())
                .item(Property.ofValue(item))
                .build()
                .run(runContextFactory.of());
        } catch (Exception e) {
            log.error("Error while deleting item", e);
        }
    }

    @SuppressWarnings("unchecked")
    private <C extends AbstractCosmosContainerTask.AbstractCosmosContainerTaskBuilder<?,?,?>> C cosmosContainerTaskBuilder(C builder) {
        return (C) applyAuth(builder
            .id(this.getClass().getSimpleName())
            .endpoint(Property.ofValue(endpoint))
            .databaseId(Property.ofValue(databaseId))
            .containerId(Property.ofValue(containerId))
        );
    }
}
