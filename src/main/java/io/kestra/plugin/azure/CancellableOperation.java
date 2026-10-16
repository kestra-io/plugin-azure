package io.kestra.plugin.azure;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks a cancellation request until the remote resource is available.
 * Dispatches cancellation asynchronously and at most once.
 */
public final class CancellableOperation {
    private static final Logger LOG = LoggerFactory.getLogger(CancellableOperation.class);

    /** Shared by Azure cancellation lifecycles and bounded Azure ML ARM calls. */
    private static final ExecutorService EXECUTOR = Executors.newCachedThreadPool(runnable -> {
        Thread thread = new Thread(runnable, "azure-cancellation");
        thread.setDaemon(true);
        return thread;
    });

    private final AtomicBoolean killed = new AtomicBoolean(false);
    private final AtomicBoolean fired = new AtomicBoolean(false);
    private final String resourceType;
    private volatile Runnable action;

    public CancellableOperation(String resourceType) {
        this.resourceType = resourceType;
    }

    public static CompletableFuture<Void> runAsync(Runnable action) {
        return CompletableFuture.runAsync(action, EXECUTOR);
    }

    public static <T> CompletableFuture<T> supplyAsync(Supplier<T> action) {
        return CompletableFuture.supplyAsync(action, EXECUTOR);
    }

    public synchronized void arm(Runnable cancelAction) {
        this.action = cancelAction;
        if (killed.get()) {
            fire();
        }
    }

    public synchronized void disarm() {
        this.action = null;
    }

    public synchronized void kill() {
        killed.set(true);
        fire();
    }

    private void fire() {
        Runnable current = action;

        if (current != null && fired.compareAndSet(false, true)) {
            runAsync(current)
                .exceptionally(e -> {
                    LOG.warn("Unexpected error while cancelling an {}", resourceType, e);
                    return null;
                });
        }
    }
}
