package io.kestra.plugin.azure.storage.adls.update;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import io.kestra.core.models.property.Property;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.azure.storage.adls.AbstractTest;
import io.kestra.plugin.azure.storage.adls.Upload;

@EnabledIfEnvironmentVariable(named = "AZURE_CONNECTION_STRING", matches = ".+", disabledReason = "Floci Azure returns HTTP 501 for Data Lake (ADLS Gen2) operations")
class SetAccessControlTest extends AbstractTest {
    @Test
    void run() throws Exception {
        String prefix = IdUtils.create();

        Upload.Output upload = upload("adls/azure/" + prefix);

        // Acquire Lease
        SetAccessControl setAccessControlTask = SetAccessControl.builder()
            .id(SetAccessControlTest.class.getSimpleName())
            .type(SetAccessControl.class.getName())
            .endpoint(Property.ofValue(this.adlsEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .fileSystem(Property.ofValue(this.fileSystem))
            .filePath(Property.ofValue(upload.getFile().getName()))
            .groupPermissions(
                SetAccessControl.Permission.builder()
                    .readPermission(Property.ofValue(true))
                    .build()
            )
            .ownerPermissions(
                SetAccessControl.Permission.builder()
                    .readPermission(Property.ofValue(true))
                    .writePermission(Property.ofValue(true))
                    .build()
            )
            .otherPermissions(null)
            .build();

        setAccessControlTask.run(runContext(setAccessControlTask));
    }
}
