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

    /**
     * Clears a previously armed action, e.g. after job submission itself failed — there is then nothing of this
     * execution's making to cancel, and leaving the action armed risks a later kill signal acting on an unrelated
     * job that happens to hold the same name (a submit failing on a 409 name collision). Does nothing once the
     * action has already fired, since that cancellation attempt already happened and cannot be undone.
     */
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
            current.run();
        }
    }
}
