package io.kestra.plugin.azure.storage.adls.update;


import io.kestra.core.models.property.Property;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.azure.storage.adls.AbstractTest;
import io.kestra.plugin.azure.storage.adls.Upload;
import org.junit.jupiter.api.Test;

class SetAccessControlTest extends AbstractTest {
    @Test
    void run() throws Exception {
        String prefix = IdUtils.create();

        Upload.Output upload = upload("adls/azure/" + prefix);

        // Acquire Lease
        SetAccessControl setAccessControlTask = SetAccessControl.builder()
            .id(SetAccessControlTest.class.getSimpleName())
            .type(SetAccessControl.class.getName())
            .endpoint(this.adlsEndpoint)
            .connectionString(this.connectionString)
            .fileSystem(this.fileSystem)
            .filePath(upload.getFile().getName())
            .groupPermissions(SetAccessControl.Permission.builder()
                .readPermission(Property.of(true))
                .build()
            )
            .ownerPermissions(SetAccessControl.Permission.builder()
                .readPermission(Property.of(true))
                .writePermission(Property.of(true))
                .build()
            )
            .otherPermissions(null)
            .build();

        setAccessControlTask.run(runContext(setAccessControlTask));
    }
}
