@PluginSubGroup(
    description = "This sub-group of plugins contains tasks for using Azure Blob Storage. \n" +
        "Azure Blob Storage includes object, file, disk, queue, and table storage.",
        categories = { PluginSubGroup.PluginCategory.CLOUD, PluginSubGroup.PluginCategory.DATA, PluginSubGroup.PluginCategory.INFRASTRUCTURE }
)
package io.kestra.plugin.azure.storage.blob;

import io.kestra.core.models.annotations.PluginSubGroup;