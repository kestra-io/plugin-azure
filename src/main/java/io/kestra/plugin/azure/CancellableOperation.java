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

    private final AtomicBoolean killed = new AtomicBoolean(false);
    private final AtomicBoolean fired = new AtomicBoolean(false);
    private final String resourceType;
    private volatile Runnable action;

    public CancellableOperation(String resourceType) {
        this.resourceType = resourceType;
    }

    /**
     * Creates a dedicated daemon executor for an individual operation.
     */
    private static ExecutorService newExecutor() {
        return Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "azure-cancellation");
            thread.setDaemon(true);
            return thread;
        });
    }

    public static CompletableFuture<Void> runAsync(Runnable action) {
        ExecutorService executor = newExecutor();
        CompletableFuture<Void> future = CompletableFuture.runAsync(action, executor);

        future.whenComplete((result, error) -> {
            if (future.isCancelled()) {
                executor.shutdownNow();
            } else {
                executor.shutdown();
            }
        });

        return future;
    }

    public static <T> CompletableFuture<T> supplyAsync(Supplier<T> action) {
        ExecutorService executor = newExecutor();
        CompletableFuture<T> future = CompletableFuture.supplyAsync(action, executor);

        future.whenComplete((result, error) -> {
            if (future.isCancelled()) {
                executor.shutdownNow();
            } else {
                executor.shutdown();
            }
        });

        return future;
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
            Thread.ofVirtual()
                .name("azure-cancellation")
                .start(() -> {
                    try {
                        current.run();
                    } catch (Throwable e) {
                        LOG.warn("Failed to cancel {}.", resourceType, e);
                    }
                });
        }
    }
}
