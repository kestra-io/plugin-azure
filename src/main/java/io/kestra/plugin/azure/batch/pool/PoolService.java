package io.kestra.plugin.azure.batch.pool;

import com.microsoft.azure.batch.BatchClient;
import com.microsoft.azure.batch.DetailLevel;
import com.microsoft.azure.batch.protocol.models.AllocationState;
import com.microsoft.azure.batch.protocol.models.CloudPool;
import com.microsoft.azure.batch.protocol.models.ComputeNode;
import io.kestra.core.runners.RunContext;

import java.io.IOException;
import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class PoolService {
    public static CloudPool waitForReady(RunContext runContext, BatchClient client, CloudPool pool) throws TimeoutException, InterruptedException, IOException {
        Duration poolSteadyTimeout = Duration.ofMinutes(5);
        Duration vmReadyTimeout = Duration.ofMinutes(20);
        long startTime = System.currentTimeMillis();
        long elapsedTime = 0L;
        boolean steady = false;

        // Wait for the VM to be allocated
        runContext.logger().trace("Waiting for pool to resize.");
        while (elapsedTime < poolSteadyTimeout.toMillis()) {
            pool = client.poolOperations().getPool(pool.id());

            if (pool.allocationState() == AllocationState.STEADY) {
                steady = true;
                break;
            }
            runContext.logger().debug(".");
            TimeUnit.SECONDS.sleep(10);
            elapsedTime = (new Date()).getTime() - startTime;
        }

        if (!steady) {
            throw new TimeoutException("The pool did not reach a steady state in the allotted time");
        }

        runContext.logger().info("The vm were allocated");

        // The VMs in the pool don't need to be in and IDLE state in order to submit a job.
        // The following code is just an example of how to poll for the VM state
        startTime = System.currentTimeMillis();
        elapsedTime = 0L;
        boolean hasIdleVM = false;

        // Wait for at least 1 VM to reach the IDLE state
        runContext.logger().trace("Waiting for VMs to start.");
        while (elapsedTime < vmReadyTimeout.toMillis()) {
            List<ComputeNode> nodeCollection = client
                .computeNodeOperations()
                .listComputeNodes(
                    pool.id(),
                    new DetailLevel.Builder().withSelectClause("id, state").withFilterClause("state eq 'idle'").build()
                );

            if (!nodeCollection.isEmpty()) {
                hasIdleVM = true;
                break;
            }

            runContext.logger().debug(".");
            TimeUnit.SECONDS.sleep(10);
            elapsedTime = (new Date()).getTime() - startTime;
        }

        if (!hasIdleVM) {
            throw new TimeoutException("The node did not reach an IDLE state in the allotted time");
        }

        runContext.logger().info("The vm were allocated");

        return pool;
    }
}
