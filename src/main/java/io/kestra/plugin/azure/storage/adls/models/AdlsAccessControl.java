package io.kestra.plugin.azure.storage.adls.models;

import com.azure.storage.file.datalake.models.PathAccessControlEntry;
import com.azure.storage.file.datalake.models.PathPermissions;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Value;

import java.util.List;

@Builder
@Value
public class AdlsAccessControl {
    List<PathAccessControlEntry> accessControlList;
    String group;
    @Schema(
        description = "Permissions for the owner."
    )
    String owner;
    PathPermissions permissions;
}
