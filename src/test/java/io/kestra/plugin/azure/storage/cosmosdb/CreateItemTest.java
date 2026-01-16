package io.kestra.plugin.azure.storage.cosmosdb;

import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.models.CosmosItemResponse;
import io.kestra.core.junit.annotations.ExecuteFlow;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;


@KestraTest(startRunner = true)
class CreateItemTest extends CosmosDbBaseTest {



    @Test
    void shouldCallCosmosContainerCreateItemAndReturnValidCosmosItemResponseGivenValidFlowProperties() throws Exception {
        //region GIVEN
        Map<String, Object> item = Map.of("key", "value");

        CreateItem createItem = CreateItem.builder()
            .id(CreateItem.class.getSimpleName())
            .endpoint(Property.ofValue("dummy-cosmos-endpoint"))
            .tenantId(Property.ofValue("dummy-tenant-id"))
            .clientId(Property.ofValue("dummy-client-id"))
            .clientSecret(Property.ofValue("dummy-client-secret"))
            .containerId(Property.ofValue(containerId))
            .item(Property.ofValue(item))
            .build();

        CosmosDatabase cosmosDatabase = mockDatabase();
        CosmosContainer cosmosContainer = mockContainer(cosmosDatabase);
        CosmosItemResponse mockCosmosItemResponse = Mockito.mock(CosmosItemResponse.class);

        Mockito.when(cosmosContainer.createItem(Mockito.eq(item))).thenReturn(mockCosmosItemResponse);

        RunContext runContext = runContextFactory.of();
        //endregion

        //region WHEN
        CreateItem.Output output = createItem.run(runContext, cosmosDatabase);
        //endregion

        //region THEN
        assertThat(output.cosmosItemResponse()).isEqualTo(mockCosmosItemResponse);
        //endregion
    }

    @Test
    @ExecuteFlow("flows/cosmos-create-item.yaml")
    void run(Execution execution) {
        assertThat(execution.getState().getCurrent()).isEqualTo(State.Type.SUCCESS);
    }
}
