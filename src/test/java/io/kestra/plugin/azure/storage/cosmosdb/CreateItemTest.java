package io.kestra.plugin.azure.storage.cosmosdb;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.utils.IdUtils;
import org.assertj.core.api.AbstractThrowableAssert;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CreateItemTest extends CosmosContainerBaseTest<CreateItem.CreateItemBuilder<?, ?>> {
    private final String testId = IdUtils.create();

    @Override
    protected CreateItem.CreateItemBuilder<?, ?> instantiateBaseTaskBuilder() {
        return CreateItem.builder();
    }

    @Test
    void shouldCreateNewItem() throws Exception {
        //region GIVEN
        Map<String, Object> item = Map.of(
            "id", "simple-create-test" + testId,
            "key", "value"
        );

        CreateItem createItem = getBaseTaskBuilder()
            .item(Property.ofValue(item))
            .build();
        //endregion

        //region WHEN
        CreateItem.Output itemResponseOutput = createItem.run(
            runContextFactory.of()
        );
        //endregion

        //region THEN
        assertThat(itemResponseOutput.statusCode()).isEqualTo(201);
        assertThat(itemResponseOutput.item().get("key")).isEqualTo("value");
        deleteItem(itemResponseOutput.item());
        //endregion
    }

    @Test
    void shouldThrowErrorWhenItemsNotSet() {
        //region GIVEN
        CreateItem createItem = getBaseTaskBuilder().build();
        //endregion

        //region WHEN
        AbstractThrowableAssert<?, ?> throwableAssert = assertThatThrownBy(() -> createItem.run(
            runContextFactory.of())
        );

        //endregion

        //region THEN
        throwableAssert.isInstanceOf(IllegalVariableEvaluationException.class);
        throwableAssert.hasMessageContaining("item cannot be empty");
        //endregion
    }
}
