package io.kestra.plugin.azure.aifoundry;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tracks a cancellation request until the Azure run ID is available. */
final class CancellableAgentRun {
    private static final Logger LOG = LoggerFactory.getLogger(CancellableAgentRun.class);

    private final AtomicBoolean killed = new AtomicBoolean(false);
    private final AtomicBoolean fired = new AtomicBoolean(false);
    private volatile Runnable action;

    synchronized void arm(Runnable cancelAction) {
        this.action = cancelAction;
        if (killed.get()) {
            fire();
        }
    }

    synchronized void disarm() {
        this.action = null;
    }

    synchronized void kill() {
        killed.set(true);
        fire();
    }

    private void fire() {
        Runnable current = action;

        if (current != null && fired.compareAndSet(false, true)) {
            CompletableFuture.runAsync(current)
                .exceptionally(e -> {
                    LOG.warn(
                        "Unexpected error while cancelling an Azure AI Foundry agent run",
                        e
                    );
                    return null;
                });
        }
    }
}
