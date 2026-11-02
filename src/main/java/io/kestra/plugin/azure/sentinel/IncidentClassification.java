package io.kestra.plugin.azure.sentinel;

/** Constant names match the Azure SecurityInsights API values verbatim, so they are serialized as is. */
public enum IncidentClassification {
    Undetermined,
    TruePositive,
    BenignPositive,
    FalsePositive
}
