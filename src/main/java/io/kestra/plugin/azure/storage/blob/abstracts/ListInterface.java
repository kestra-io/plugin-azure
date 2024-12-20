package io.kestra.plugin.azure.storage.blob.abstracts;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.validation.constraints.NotNull;

public interface ListInterface {
    @Schema(
        title = "Limits the response to keys that begin with the specified prefix."
    )
    Property<String> getPrefix();

    @Schema(
        title = "A regular expression to filter on the full key.",
        description = "ex:\n"+
            "`regExp: .*` to match all files\n"+
            "`regExp: .*2020-01-0.\\\\.csv` to match files between 01 and 09 of january ending with `.csv`"
    )
    Property<String> getRegexp();

    @Schema(
        title = "The delimiter for blob hierarchy, \"/\" for hierarchy based on directories."
    )
    Property<String> getDelimiter();

    @Schema(
        title = "The filter for files or directories."
    )
    @NotNull
    Property<Filter> getFilter();

    enum Filter {
        FILES,
        DIRECTORY,
        BOTH
    }
}
