package io.kestra.plugin.azure.batch.pool;

import com.microsoft.azure.batch.BatchClient;
import com.microsoft.azure.batch.protocol.models.CloudPool;
import com.microsoft.azure.batch.protocol.models.PoolState;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.batch.AbstractBatch;
import io.kestra.plugin.azure.batch.models.Pool;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            code = {
                "poolId: \"<your-pool-id>\"",
                "targetDedicatedNodes: \"12\"",
            }
        )
    }
)
@Schema(
    title = "Resize a Azure Batch pool."
)
public class Resize extends AbstractBatch implements RunnableTask<Resize.Output> {
    @Schema(
        title = "The ID of the pool."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String poolId;

    @Schema(
        title = "The desired number of dedicated compute nodes in the pool."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    @Builder.Default
    private Integer targetDedicatedNodes = 0;

    @Schema(
        title = "The desired number of low-priority compute nodes in the pool."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    @Builder.Default
    private Integer targetLowPriorityNodes = 0;

    @Override
    public Output run(RunContext runContext) throws Exception {
        BatchClient client = this.client(runContext);

        String poolId = runContext.render(this.poolId);

        if (!client.poolOperations().existsPool(poolId)) {
            throw new IllegalArgumentException("Pool '" + poolId + "' doesn't exists");
        }

        CloudPool pool = client.poolOperations().getPool(poolId);

        if (!pool.state().equals(PoolState.ACTIVE)) {
            throw new IllegalArgumentException("Pool '" + poolId + "' is not active, state: '" + pool.state() + "'");
        }

        client
            .poolOperations()
            .resizePool(poolId, this.targetDedicatedNodes, this.targetLowPriorityNodes);

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
