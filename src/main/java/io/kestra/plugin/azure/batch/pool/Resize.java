package io.kestra.plugin.azure.batch.pool;

import com.microsoft.azure.batch.BatchClient;
import com.microsoft.azure.batch.protocol.models.CloudPool;
import com.microsoft.azure.batch.protocol.models.PoolState;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.batch.AbstractBatch;
import io.kestra.plugin.azure.batch.BatchService;
import io.kestra.plugin.azure.batch.models.Pool;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import jakarta.validation.constraints.NotNull;

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
                id: azure_batch_pool_resize
                namespace: company.team

                tasks:
                  - id: resize
                    type: io.kestra.plugin.azure.batch.pool.Resize
                    poolId: "<your-pool-id>"
                    targetDedicatedNodes: "12"
                """
        )
    }
)
@Schema(
    title = "Resize an Azure Batch pool."
)
public class Resize extends AbstractBatch implements RunnableTask<Resize.Output> {
    @Schema(
        title = "The ID of the pool."
    )
    @NotNull
    private Property<String> poolId;

    @Schema(
        title = "The desired number of dedicated compute nodes in the pool."
    )
    @NotNull
    @Builder.Default
    private Property<Integer> targetDedicatedNodes = Property.ofValue(0);

    @Schema(
        title = "The desired number of low-priority compute nodes in the pool."
    )
    @NotNull
    @Builder.Default
    private Property<Integer> targetLowPriorityNodes = Property.ofValue(0);

    @Override
    public Output run(RunContext runContext) throws Exception {
        BatchClient client = BatchService.client(this.endpoint, this.account, this.accessKey, runContext);

        String poolId = runContext.render(this.poolId).as(String.class).orElseThrow();

        if (!client.poolOperations().existsPool(poolId)) {
            throw new IllegalArgumentException("Pool '" + poolId + "' doesn't exists");
        }

        CloudPool pool = client.poolOperations().getPool(poolId);

        if (!pool.state().equals(PoolState.ACTIVE)) {
            throw new IllegalArgumentException("Pool '" + poolId + "' is not active, state: '" + pool.state() + "'");
        }

        client
            .poolOperations()
            .resizePool(
                poolId,
                runContext.render(this.targetDedicatedNodes).as(Integer.class).orElseThrow(),
                runContext.render(this.targetLowPriorityNodes).as(Integer.class).orElseThrow()
            );

        CloudPool cloudPool = PoolService.waitForReady(runContext, client, pool);

        return Output
            .builder()
            .pool(Pool.builder()
                .id(cloudPool.id())
                .targetDedicatedNodes(cloudPool.targetDedicatedNodes())
                .targetLowPriorityNodes(cloudPool.targetLowPriorityNodes())
                .build()
            )
            .build();
    }

    @SuperBuilder
    @Getter
    @NoArgsConstructor
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The pool that has been resized."
        )
        private Pool pool;
    }
}
