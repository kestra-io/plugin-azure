package io.kestra.plugin.azure.storage.cosmosdb;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import org.assertj.core.api.AbstractThrowableAssert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class BatchTest extends CosmosContainerBaseTest<Batch.BatchBuilder<?,?>> {
    List<Map<String, Object>> items = List.of(
        Map.of(
            "id", "batch-create-test-one" + testId,
            "pk", "test",
            "key", "value"
        ),
        Map.of(
            "id", "batch-create-test-two" + testId,
            "pk", "test",
            "key", "value"
        )
    );

    @AfterAll
    void afterAll() {
        items.forEach(this::deleteItem);
    }

    @Override
    protected Batch.BatchBuilder<?, ?> instantiateBaseTaskBuilder() {
        return Batch.builder();
    }

    @Test
    void shouldBatchCreateItems() throws Exception {
        //region GIVEN
        Batch batch = getBaseTaskBuilder()
            .partitionKeyValue(Property.ofValue("test"))
            .items(Property.ofValue(items))
            .build();
        //endregion

        //region WHEN
        Batch.BatchResponseOutput output = batch.run(runContextFactory.of());
        //endregion

        //region WHEN
        assertThat(output.statusCode()).isEqualTo(200);
        //endregion
    }

    @Test
    void shouldThrowErrorWhenPartitionKeyNotSet() {
        //region GIVEN
        Batch batch = getBaseTaskBuilder()
            .items(Property.ofValue(items))
            .build();
        //endregion

        //region WHEN
        AbstractThrowableAssert<?, ?> throwableAssert = assertThatThrownBy(
            () -> batch.run(runContextFactory.of())
        );

        //endregion

        //region WHEN
        throwableAssert.isInstanceOf(IllegalVariableEvaluationException.class);
        throwableAssert.hasMessageContaining("partitionKeyValue cannot be null or empty");
        //endregion
    }

    @Test
    void shouldThrowErrorWhenItemsNotSet() {
        //region GIVEN
        Batch batch = getBaseTaskBuilder()
            .build();
        //endregion

        //region WHEN
        AbstractThrowableAssert<?, ?> throwableAssert = assertThatThrownBy(
            () -> batch.run(runContextFactory.of())
        );

        //endregion

        //region WHEN
        throwableAssert.isInstanceOf(IllegalVariableEvaluationException.class);
        throwableAssert.hasMessageContaining("partitionKeyValue cannot be null or empty");
        //endregion
    }
}