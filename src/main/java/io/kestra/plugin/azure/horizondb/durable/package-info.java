@PluginSubGroup(
    title = "HorizonDB Durable Workflows",
    description = "This sub-group of plugins contains tasks and triggers for managing pg_durable durable function " +
        "workflows on Azure HorizonDB: submitting, cancelling, signalling, inspecting, listing, and reacting to " +
        "durable function instances.",
    categories = { PluginSubGroup.PluginCategory.CLOUD }
)
package io.kestra.plugin.azure.horizondb.durable;

import io.kestra.core.models.annotations.PluginSubGroup;
