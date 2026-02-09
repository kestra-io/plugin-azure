package io.kestra.plugin.azure.storage.blob;

import com.azure.storage.blob.models.BlobStorageException;
import com.google.common.io.CharStreams;
import io.kestra.core.models.property.Property;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AllTest extends AbstractTest {
    @Test
    void run() throws Exception {
        String prefix = IdUtils.create();

        Upload.Output upload = upload("tasks/azure/" + prefix);

        // list
        List list = List.builder()
            .id(AllTest.class.getSimpleName())
            .type(Upload.class.getName())
            .endpoint(Property.ofValue(this.storageEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .container(Property.ofValue(this.container))
            .prefix(Property.ofValue("tasks/azure/" + prefix  + "/"))
            .build();

        List.Output listOutput = list.run(runContext(list));
        assertThat(listOutput.getBlobs().size(), is(1));

        // download
        Download download = Download.builder()
            .id(AllTest.class.getSimpleName())
            .type(Download.class.getName())
            .endpoint(Property.ofValue(this.storageEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .container(Property.ofValue(this.container))
            .name(Property.ofValue(upload.getBlob().getName()))
            .build();
        Download.Output run = download.run(runContext(download));

        InputStream get = storageInterface.get(TenantService.MAIN_TENANT, null, run.getBlob().getUri());
        assertThat(
            CharStreams.toString(new InputStreamReader(get)),
            is(CharStreams.toString(new InputStreamReader(new FileInputStream(file("application.yml")))))
        );

        // delete
        Delete delete = Delete.builder()
            .id(AllTest.class.getSimpleName())
            .type(Delete.class.getName())
            .endpoint(Property.ofValue(this.storageEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .container(Property.ofValue(this.container))
            .name(Property.ofValue(upload.getBlob().getName()))
            .build();
        Delete.Output deleteOutput = delete.run(runContext(delete));
        assertThat(deleteOutput.getBlob(), is(notNullValue()));

        // delete missing
        BlobStorageException blobStorageException = assertThrows(
            BlobStorageException.class,
            () -> download.run(runContext(download))
        );

        assertThat(blobStorageException.getStatusCode(), is(404));
    }

    @Test
    void maxFiles() throws Exception {
        String prefix = IdUtils.create();

        for (int i = 0; i < 30; i++) {
            upload("tasks/azure/" + prefix);
        }

        // Test with explicit maxFiles set to 50 - should return all 30 files
        List listWithHighLimit = List.builder()
            .id(AllTest.class.getSimpleName())
            .type(List.class.getName())
            .endpoint(Property.ofValue(this.storageEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .container(Property.ofValue(this.container))
            .prefix(Property.ofValue("tasks/azure/" + prefix + "/"))
            .maxFiles(Property.ofValue(50))
            .build();

        List.Output listWithHighLimitOutput = listWithHighLimit.run(runContext(listWithHighLimit));
        assertThat(listWithHighLimitOutput.getBlobs().size(), is(30));

        // Test with default maxFiles (25) - should return only 25 files
        List listWithDefaultLimit = List.builder()
            .id(AllTest.class.getSimpleName())
            .type(List.class.getName())
            .endpoint(Property.ofValue(this.storageEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .container(Property.ofValue(this.container))
            .prefix(Property.ofValue("tasks/azure/" + prefix + "/"))
            .build();

        List.Output listWithDefaultLimitOutput = listWithDefaultLimit.run(runContext(listWithDefaultLimit));
        assertThat(listWithDefaultLimitOutput.getBlobs().size(), is(25));
    }
}
