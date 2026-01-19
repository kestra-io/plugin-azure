package io.kestra.plugin.azure.storage.cosmosdb;

import io.kestra.core.models.property.Property;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class DeleteTest extends CosmosContainerBaseTest<Delete.DeleteBuilder<?, ?>> {

    @Override
    protected Delete.DeleteBuilder<?, ?> instantiateBaseTaskBuilder() {
        return Delete.builder();
    }

    @Test
    void shouldDeleteItem() throws Exception {
        //region GIVEN
        Map<String, Object> item = createItem("delete-item-test", Map.of(), false);


        final Delete delete = getBaseTaskBuilder()
            .item(Property.ofValue(item))
            .build();
        //endregion

        //region WHEN
        AbstractCosmosContainerTask.ItemResponseOutput<?> deleteItemResponseOutput =  delete.run(
            runContextFactory.of()
        );
        //endregion

        //region THEN
        assertThat(deleteItemResponseOutput.statusCode()).isEqualTo(204);
        //endregion
    }


}