package io.kestra.plugin.azure.storage.cosmosdb;

import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.models.CosmosItemResponse;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: azure_cosmos_container_create_item
                namespace: company.team

                tasks:
                  - id: create
                    type: io.kestra.plugin.azure.storage.cosmosdb.CreateItem
                    endpoint: "https://yourcosmosaccount.documents.azure.com"
                    databaseId: your_data_base_id
                    containerId: your_container_id
                    tenantId: "{{ secret('AZURE_TENANT_ID') }}"
                    clientId: "{{ secret('AZURE_CLIENT_ID') }}"
                    clientSecret: "{{ secret('AZURE_CLIENT_SECRET') }}"
                    item:
                        key: value
                """
        )
    }
)
@Schema(
    title = "Creates a new Cosmos item and returns its respective Cosmos item response."
)
public class CreateItem extends AbstractCosmosContainerTask<CreateItem.Output> {

    @NotNull
    @Schema(title = "container ID")
    private Property<Map<String, Object>> item;

    @Override
    protected Output run(RunContext runContext, CosmosContainer cosmosContainer) throws IllegalVariableEvaluationException {
        Map<String, Object> rItem = runContext.render(item).asMap(String.class, Object.class);
        return new Output(cosmosContainer.createItem(rItem));
    }

    public record Output(
        CosmosItemResponse<Map<String, Object>> cosmosItemResponse
    ) implements io.kestra.core.models.tasks.Output { }
}
