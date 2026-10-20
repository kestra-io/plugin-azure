package io.kestra.plugin.azure;

import java.util.concurrent.atomic.AtomicBoolean;

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
                .start(() ->
                {
                    try {
                        current.run();
                    } catch (Throwable e) {
                        LOG.warn("Failed to cancel {}.", resourceType, e);
                    }
                });
        }
    }
}
