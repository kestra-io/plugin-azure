package io.kestra.plugin.azure.storage.cosmosdb;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import org.assertj.core.api.AbstractThrowableAssert;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@KestraTest
class QueryTest extends CosmosContainerBaseTest<Query.QueryBuilder<?, ?>> {
    @Override
    protected Query.QueryBuilder<?, ?> instantiateBaseTaskBuilder() {
         return Query.builder();
    }

    @Test
    void shouldReturnQueriedForItems() throws Exception {
        //region GIVEN
        Map<String, Object> item = createItem("query-test", Map.of("key", "value"));

        Query query = getBaseTaskBuilder()
            .query(Property.ofValue("SELECT * FROM c WHERE c.id = '" + item.get("id") + "'"))
            .build();
        //endregion

        //region WHEN
        final RunContext runContext = runContextFactory.of();

        Query.Output output = query.run(runContext);
        //endregion

        //region WHEN
        assertThat(output.queryResults().getFirst()).isEqualTo(item);
        //endregion
    }

    @Test
    void shouldReturnQueriedForItemsWhenPartitionKeySet() throws Exception {
        //region GIVEN
        Map<String, Object> item = createItem("query-partiton-key-test", Map.of(
            "key", "value",
            "pk", "test"
        ));

        Query query = getBaseTaskBuilder()
            .query(Property.ofValue("SELECT * FROM c WHERE c.id = '" + item.get("id") + "'"))
            .partitionKey(Property.ofValue(Map.of("pk", "test")))
            .partitionKeyDefinition(Property.ofValue(PARTITION_KEY_DEFINITION))
            .build();
        //endregion

        //region WHEN
        final RunContext runContext = runContextFactory.of();

        Query.Output output = query.run(runContext);
        //endregion

        //region WHEN
        assertThat(output.queryResults().getFirst()).isEqualTo(item);
        //endregion
    }

    @Test
    void shouldReturnQueriedForItemsWhenFeedRangeSet() throws Exception {
        //region GIVEN
        Map<String, Object> item = createItem("query-feed-range-test", Map.of(
            "key", "value",
            "pk", "test"
        ));

        Query query = getBaseTaskBuilder()
            .query(Property.ofValue("SELECT * FROM c WHERE c.id = '" + item.get("id") + "'"))
            .feedRangePartitionKey(Property.ofValue(Map.of("pk", "test")))
            .partitionKeyDefinition(Property.ofValue(PARTITION_KEY_DEFINITION))
            .build();
        //endregion

        //region WHEN
        final RunContext runContext = runContextFactory.of();

        Query.Output output = query.run(runContext);
        //endregion

        //region WHEN
        assertThat(output.queryResults().getFirst()).isEqualTo(item);
        //endregion
    }

    @Test
    void shouldThrowErrorWhenFeedRangeSetWithoutPartitionDefinition() {
        //region GIVEN

        Query query = getBaseTaskBuilder()
            .query(Property.ofValue("SELECT * FROM c WHERE c.id = '404'"))
            .feedRangePartitionKey(Property.ofValue(Map.of("pk", "test")))
            .build();
        //endregion

        //region WHEN
        final RunContext runContext = runContextFactory.of();

        AbstractThrowableAssert<?, ?> throwableAssert = assertThatThrownBy(() -> query.run(runContext));
        //endregion

        //region WHEN
        throwableAssert.isInstanceOf(IllegalVariableEvaluationException.class);
        throwableAssert.hasMessageContaining(
            "PartitionKeyDefinition must be set when partitionKey or feedRangePartitionKey is set"
        );
        //endregion
    }

    @Test
    void shouldThrowErrorWhenPartitionKeySetWithoutPartitionDefinition() {
        //region GIVEN
        Query query = getBaseTaskBuilder()
            .query(Property.ofValue("SELECT * FROM c WHERE c.id = '404'"))
            .partitionKey(Property.ofValue(Map.of("pk", "test")))
            .build();
        //endregion

        //region WHEN
        final RunContext runContext = runContextFactory.of();

        AbstractThrowableAssert<?, ?> throwableAssert = assertThatThrownBy(() -> query.run(runContext));
        //endregion

        //region WHEN
        throwableAssert.isInstanceOf(IllegalVariableEvaluationException.class);
        throwableAssert.hasMessageContaining(
            "PartitionKeyDefinition must be set when partitionKey or feedRangePartitionKey is set"
        );
        //endregion
    }

    @Test
    void shouldThrowErrorWhenPartitionDefinitionSetWithOutPartitionKeyOrFeedRange() {
        //region GIVEN
        Query query = getBaseTaskBuilder()
            .query(Property.ofValue("SELECT * FROM c WHERE c.id = '404'"))
            .partitionKeyDefinition(Property.ofValue(PARTITION_KEY_DEFINITION))
            .build();
        //endregion

        //region WHEN
        final RunContext runContext = runContextFactory.of();

        AbstractThrowableAssert<?, ?> throwableAssert = assertThatThrownBy(() -> query.run(runContext));
        //endregion

        //region WHEN
        throwableAssert.isInstanceOf(IllegalVariableEvaluationException.class);
        throwableAssert.hasMessageContaining(
            "When partitionKeyDefinition is set, " +
                "exactly one of partitionKey or feedRangePartitionKey must be set"
        );
        //endregion
    }
}