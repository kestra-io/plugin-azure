package io.kestra.plugin.azure.storage.blob.abstracts;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;

import javax.validation.constraints.NotNull;

public interface ListInterface {
    @Schema(
        title = "Limits the response to keys that begin with the specified prefix."
    )
    @PluginProperty(dynamic = true)
    String getPrefix();

    @Schema(
        title = "A regexp to filter on full key",
        description = "ex:\n"+
            "`regExp: .*` to match all files\n"+
            "`regExp: .*2020-01-0.\\\\.csv` to match files between 01 and 09 of january ending with `.csv`"
    )
    @PluginProperty(dynamic = true)
    String getRegexp();

    @Schema(
        title = "The delimiter for blob hierarchy, \"/\" for hierarchy based on directories."
    )
    @PluginProperty(dynamic = true)
    String getDelimiter();

    @Schema(
        title = "The filter files or directory"
    )
    @NotNull
    Filter getFilter();

    enum Filter {
        FILES,
        DIRECTORY,
        BOTH
    }
}
