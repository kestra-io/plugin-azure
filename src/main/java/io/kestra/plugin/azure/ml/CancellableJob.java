package io.kestra.plugin.azure.ml;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Backs the {@code kill()} lifecycle hook shared by {@link SubmitCommandJob} and {@link SubmitPipelineJob}: once the
 * remote job id is known, {@link #arm(Runnable)} records how to cancel it, and {@link #kill()} runs that action at
 * most once, guarding against a duplicate or racing kill signal.
 */
final class CancellableJob {
    private final AtomicReference<Runnable> action = new AtomicReference<>();
    private final AtomicBoolean killed = new AtomicBoolean(false);

    void arm(Runnable cancelAction) {
        action.set(cancelAction);
    }

    void kill() {
        if (killed.compareAndSet(false, true)) {
            Optional.ofNullable(action.get()).ifPresent(Runnable::run);
        }
    }
}
