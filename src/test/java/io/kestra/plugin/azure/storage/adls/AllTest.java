package io.kestra.plugin.azure.storage.adls;

import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.google.common.io.CharStreams;
import io.kestra.core.models.property.Property;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@EnabledIfEnvironmentVariable(named = "AZURE_CONNECTION_STRING", matches = ".+")
class AllTest extends AbstractTest {
    @Test
    void run() throws Exception {
        String prefix = IdUtils.create();

        Upload.Output upload = upload("adls/azure/" + prefix);

        // list
        List list = List.builder()
            .id(AllTest.class.getSimpleName())
            .type(io.kestra.plugin.azure.storage.blob.Upload.class.getName())
            .endpoint(Property.ofValue(this.adlsEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .fileSystem(Property.ofValue(this.fileSystem))
            .directoryPath(Property.ofValue("adls/azure/" + prefix  + "/"))
            .build();

        List.Output listOutput = list.run(runContext(list));
        assertThat(listOutput.getFiles().size(), is(1));
        assertTrue(listOutput.getFiles().getFirst().getName().startsWith("adls/azure/" + prefix));

        // download
        Read download = Read.builder()
            .id(AllTest.class.getSimpleName())
            .type(Read.class.getName())
            .endpoint(Property.ofValue(this.adlsEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .fileSystem(Property.ofValue(this.fileSystem))
            .filePath(Property.ofValue(upload.getFile().getName()))
            .build();

        Read.Output run = download.run(runContext(download));

        InputStream get = storageInterface.get(TenantService.MAIN_TENANT, null, run.getFile().getUri());

        assertThat(
            CharStreams.toString(new InputStreamReader(get)),
            is(CharStreams.toString(new InputStreamReader(new FileInputStream(file("application.yml")))))
        );

        // delete
        Delete delete = Delete.builder()
            .id(AllTest.class.getSimpleName())
            .type(Delete.class.getName())
            .endpoint(Property.ofValue(this.adlsEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .fileSystem(Property.ofValue(this.fileSystem))
            .filePath(Property.ofValue(upload.getFile().getName()))
            .build();

        Delete.Output deleteOutput = delete.run(runContext(delete));
        assertThat(deleteOutput.getFile(), is(notNullValue()));

        // delete missing
        DataLakeStorageException blobStorageException = assertThrows(
            DataLakeStorageException.class,
            () -> download.run(runContext(download))
        );

        assertThat(blobStorageException.getStatusCode(), is(404));
    }
}
