package io.kestra.plugin.azure.storage.cosmosdb;

import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.mockito.Mockito;

public abstract class CosmosDbBaseTest {
    protected static final String containerId = "containerId";

    @Inject
    protected RunContextFactory runContextFactory = new RunContextFactory();


    protected CosmosDatabase mockDatabase() {
        return Mockito.mock(CosmosDatabase.class);
    }

    protected CosmosContainer mockContainer(CosmosDatabase cosmosDatabase) {
        CosmosContainer cosmosContainer = Mockito.mock(CosmosContainer.class);
        Mockito.when(cosmosDatabase.getContainer(Mockito.eq(containerId))).thenReturn(cosmosContainer);
        return cosmosContainer;
    }
}
