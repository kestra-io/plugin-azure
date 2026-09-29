package io.kestra.plugin.azure.ml;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Backs the {@code kill()} lifecycle hook shared by {@link SubmitCommandJob} and {@link SubmitPipelineJob}: once the
 * remote job id is known, {@link #arm(Runnable)} records how to cancel it, and {@link #kill()} runs that action at
 * most once, guarding against a duplicate or racing kill signal. A kill signal that arrives before {@link #arm} has
 * run (e.g. while the create-job call is still in flight) is not dropped — it is remembered and applied as soon as
 * the cancel action is armed.
 *
 * <p>The cancel action itself (an ARM call that can retry for several seconds — see {@code cancelQuietly}) is
 * dispatched off-thread rather than run inline: {@code kill()} is invoked synchronously by the worker's task
 * lifecycle callback, on a thread shared with other jobs' kill signals and dispatch, and must return promptly.</p>
 */
final class CancellableJob {
    private static final Logger LOG = LoggerFactory.getLogger(CancellableJob.class);

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
            // The cancel action (cancelQuietly) already contains its own error handling; this only guards against
            // a genuinely unexpected failure inside it, which would otherwise vanish silently into an unobserved
            // future instead of at least being logged.
            CompletableFuture.runAsync(current, MachineLearningService.EXECUTOR)
                .exceptionally(e -> {
                    LOG.warn("Unexpected error while cancelling an Azure Machine Learning job", e);
                    return null;
                });
        }
    }
}
