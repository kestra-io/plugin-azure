package io.kestra.plugin.azure.storage.cosmosdb;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import org.assertj.core.api.AbstractThrowableAssert;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class QueriesTest extends CosmosContainerBaseTest<Queries.QueriesBuilder<?, ?>> {

    @Override
    protected Queries.QueriesBuilder<?, ?> instantiateBaseTaskBuilder() {
        return Queries.builder();
    }

    @Test
    void shouldReturnMapOfQueryResults() throws Exception {
        //region GIVEN
        Map<String, Object> itemOne = createItem(
            "simple-queries-test-one",
            Map.of("secondId", "simple-queries-test-one")
        );
        Map<String, Object> itemTwo = createItem(
            "simple-queries-test-two",
            Map.of("secondId", "simple-queries-test-two")
        );

        Map<String, Queries.QueriesOptions> queries = Map.of(
            "queryOne", Queries.QueriesOptions.builder()
                .query("SELECT * FROM c WHERE c.secondId = 'simple-queries-test-one'")
                .build(),
            "queryTwo", Queries.QueriesOptions.builder()
                .query("SELECT * FROM c WHERE c.secondId = 'simple-queries-test-two'")
                .build()
        );

        Queries queriesTask = getBaseTaskBuilder()
            .queries(Property.ofValue(queries))
            .build();
        //endregion

        //region WHEN
        Queries.Output queriesTaskOutput =  queriesTask.run(runContextFactory.of());
        //endregion

        //region THEN
        assertThat(queriesTaskOutput.results().get("queryOne")).isNotNull();
        assertThat(queriesTaskOutput.results().get("queryOne").getFirst()).isEqualTo(itemOne);

        assertThat(queriesTaskOutput.results().get("queryTwo")).isNotNull();
        assertThat(queriesTaskOutput.results().get("queryTwo").getFirst()).isEqualTo(itemTwo);
        //endregion
    }

    @Test
    void shouldReturnMapOfQueryResultsWhenPartitionKeySet() throws Exception {
        //region GIVEN
        Map<String, Object> itemOne = createItem(
            "partiton-key-queries-test-one",
            Map.of(
                "secondId", "partiton-key-queries-test-one",
                "pk", "test"
            )
        );

        Map<String, Queries.QueriesOptions> queries = Map.of(
            "queryOne", Queries.QueriesOptions.builder()
                .query("SELECT * FROM c WHERE c.secondId = 'partiton-key-queries-test-one'")
                    .partitionKey(Map.of("pk", "test"))
                    .partitionKeyDefinition(PARTITION_KEY_DEFINITION)
                .build()
        );

        Queries queriesTask = getBaseTaskBuilder()
            .queries(Property.ofValue(queries))
            .build();
        //endregion

        //region WHEN
        Queries.Output queriesTaskOutput =  queriesTask.run(runContextFactory.of());
        //endregion

        //region THEN
        assertThat(queriesTaskOutput.results().get("queryOne")).isNotNull();
        assertThat(queriesTaskOutput.results().get("queryOne").getFirst()).isEqualTo(itemOne);
        //endregion
    }

    @Test
    void shouldReturnMapOfQueryResultsWhenFeedRangeSet() throws Exception {
        //region GIVEN
        Map<String, Object> itemOne = createItem(
            "feed-range-queries-test-one",
            Map.of(
                "secondId", "feed-range-queries-test-one",
                "pk", "test"
            )
        );

        Map<String, Queries.QueriesOptions> queries = Map.of(
            "queryOne", Queries.QueriesOptions.builder()
                .query("SELECT * FROM c WHERE c.secondId = 'feed-range-queries-test-one'")
                .feedRangePartitionKey(Map.of("pk", "test"))
                .partitionKeyDefinition(PARTITION_KEY_DEFINITION)
                .build()
        );

        Queries queriesTask = getBaseTaskBuilder()
            .queries(Property.ofValue(queries))
            .build();
        //endregion

        //region WHEN
        Queries.Output queriesTaskOutput =  queriesTask.run(runContextFactory.of());
        //endregion

        //region THEN
        assertThat(queriesTaskOutput.results().get("queryOne")).isNotNull();
        assertThat(queriesTaskOutput.results().get("queryOne").getFirst()).isEqualTo(itemOne);
        //endregion
    }

    @Test
    void shouldThrowErrorWhenFeedRangePartitionKeySetAndPartitionKeyDefinitionNotSet() throws Exception {
        //region GIVEN
        Map<String, Object> itemOne = Map.of("id", "example_id");

        Map<String, Queries.QueriesOptions> queries = Map.of(
            "queryOne", Queries.QueriesOptions.builder()
                .query("SELECT * FROM c WHERE c.secondId = 'one'")
                .feedRangePartitionKey(Map.of("pk", "test"))
                .build()
        );

        Queries queriesTask = getBaseTaskBuilder()
            .queries(Property.ofValue(queries))
            .build();
        //endregion

        //region WHEN
        AbstractThrowableAssert<?, ?> throwableAssert = assertThatThrownBy(
            () -> queriesTask.run(runContextFactory.of())
        );
        //endregion

        //region THEN
        throwableAssert.isInstanceOf(IllegalVariableEvaluationException.class);
        throwableAssert.hasMessageContaining(
            "PartitionKeyDefinition must be set when partitionKey or feedRangePartitionKey is set"
        );
        //endregion
    }
}