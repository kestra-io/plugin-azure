package io.kestra.plugin.azure.storage.adls.update;

import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.models.PathAccessControlEntry;
import com.azure.storage.file.datalake.models.PathInfo;
import com.azure.storage.file.datalake.models.PathPermissions;
import com.azure.storage.file.datalake.models.RolePermissions;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.storage.adls.abstracts.AbstractDataLakeWithFile;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: azure_storage_datalake_set_access_control
                namespace: company.team

                tasks:
                  - id: lease_file
                    type: io.kestra.plugin.azure.storage.adls.update.SetAccessControl
                    endpoint: "https://yourblob.blob.core.windows.net"
                    sasToken: "{{ secret('SAS_TOKEN') }}"
                    fileSystem: "mydata"
                    filePath: "path/to/myfile"
                    groupPermissions:
                      readPermissions: true
                    ownerPermissions:
                      readPermissions: true
                      writePermissions: true
                    otherPermissions:
                      readPermissions: true
                """
        )
    }
)
@Schema(
    title = "Set access controls to a file in Azure Data Lake Storage."
)
public class SetAccessControl extends AbstractDataLakeWithFile implements RunnableTask<VoidOutput> {
    @Schema(
        title = "Group permissions."
    )
    private Permission groupPermissions;

    @Schema(
        title = "Owner permissions."
    )
    private Permission ownerPermissions;

    @Schema(
        title = "Other permissions."
    )
    private Permission otherPermissions;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        DataLakeFileClient client = this.dataLakeFileClient(runContext);

        runContext.logger().info("Initial permissions are : {}", PathAccessControlEntry.serializeList(client.getAccessControl().getAccessControlList()));

        RolePermissions groupPermission = new RolePermissions();
        if (this.groupPermissions != null) {
            groupPermission
                .setExecutePermission(runContext.render(groupPermissions.getExecutePermission()).as(Boolean.class).orElse(false))
                .setWritePermission(runContext.render(groupPermissions.getWritePermission()).as(Boolean.class).orElse(false))
                .setReadPermission(runContext.render(groupPermissions.getReadPermission()).as(Boolean.class).orElse(false));
        }

        RolePermissions ownerPermission = new RolePermissions();
        if (this.ownerPermissions != null) {
            ownerPermission
                .setExecutePermission(runContext.render(ownerPermissions.getExecutePermission()).as(Boolean.class).orElse(false))
                .setWritePermission(runContext.render(ownerPermissions.getWritePermission()).as(Boolean.class).orElse(false))
                .setReadPermission(runContext.render(ownerPermissions.getReadPermission()).as(Boolean.class).orElse(false));
        }

        RolePermissions otherPermission = new RolePermissions();
        if(this.otherPermissions != null) {
            otherPermission
                .setExecutePermission(runContext.render(otherPermissions.getExecutePermission()).as(Boolean.class).orElse(false))
                .setWritePermission(runContext.render(otherPermissions.getWritePermission()).as(Boolean.class).orElse(false))
                .setReadPermission(runContext.render(otherPermissions.getReadPermission()).as(Boolean.class).orElse(false));
        }

        PathPermissions permissions = new PathPermissions();

        permissions.setGroup(groupPermission);
        permissions.setOwner(ownerPermission);
        permissions.setOther(otherPermission);

        client.setPermissions(permissions, null, null);
        runContext.logger().info("Final permissions are : {}", PathAccessControlEntry.serializeList(client.getAccessControl().getAccessControlList()));

        return null;
    }

    @SuperBuilder(toBuilder = true)
    @Getter
    @NoArgsConstructor
    public static class Permission {
        Property<Boolean> readPermission;
        Property<Boolean> writePermission;
        Property<Boolean> executePermission;
    }
}
