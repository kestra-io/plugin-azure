package io.kestra.plugin.azure.ml;

/**
 * Mirrors {@code com.azure.resourcemanager.machinelearning.models.JobStatus}, exposed as a closed Java enum so the
 * Kestra UI can render it with autocompletion instead of an open-ended string.
 */
public enum JobState {
    NOT_STARTED,
    STARTING,
    PROVISIONING,
    PREPARING,
    QUEUED,
    RUNNING,
    FINALIZING,
    CANCEL_REQUESTED,
    COMPLETED,
    FAILED,
    CANCELED,
    NOT_RESPONDING,
    PAUSED,
    UNKNOWN;

    public boolean isTerminal() {
        return this == COMPLETED || this == FAILED || this == CANCELED || this == NOT_RESPONDING;
    }

    public boolean isFailure() {
        return this == FAILED || this == CANCELED || this == NOT_RESPONDING;
    }
}
