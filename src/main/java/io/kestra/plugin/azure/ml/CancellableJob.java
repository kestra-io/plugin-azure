package io.kestra.plugin.azure.ml;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Backs the {@code kill()} lifecycle hook shared by {@link SubmitCommandJob} and {@link SubmitPipelineJob}: once the
 * remote job id is known, {@link #arm(Runnable)} records how to cancel it, and {@link #kill()} runs that action at
 * most once, guarding against a duplicate or racing kill signal. A kill signal that arrives before {@link #arm} has
 * run (e.g. while the create-job call is still in flight) is not dropped — it is remembered and applied as soon as
 * the cancel action is armed.
 */
final class CancellableJob {
    private final AtomicBoolean killed = new AtomicBoolean(false);
    private final AtomicBoolean fired = new AtomicBoolean(false);
    private volatile Runnable action;

    synchronized void arm(Runnable cancelAction) {
        this.action = cancelAction;
        if (killed.get()) {
            fire();
        }
    }

    synchronized void kill() {
        killed.set(true);
        fire();
    }

    private void fire() {
        Runnable current = action;
        if (current != null && fired.compareAndSet(false, true)) {
            current.run();
        }
    }
}
