package io.kestra.plugin.azure.ml;

/**
 * Where the model artifact registered by {@link RegisterModel} comes from.
 */
public enum ModelSource {
    JOB_OUTPUT,
    INTERNAL_STORAGE,
    DATASTORE_URI
}
