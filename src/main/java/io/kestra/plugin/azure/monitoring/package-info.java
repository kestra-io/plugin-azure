@PluginSubGroup(
    description = "This sub-group of plugins contains tasks for using Azure Monitor. \n" +
        "Azure Monitor provides full-stack observability across applications, infrastructure, and networks, " +
        "and enables querying and pushing metrics using Azure Monitor Query and Ingestion APIs.",
        categories = { PluginSubGroup.PluginCategory.CLOUD, PluginSubGroup.PluginCategory.DATA, PluginSubGroup.PluginCategory.INFRASTRUCTURE }
)
package io.kestra.plugin.azure.monitoring;

import io.kestra.core.models.annotations.PluginSubGroup;
