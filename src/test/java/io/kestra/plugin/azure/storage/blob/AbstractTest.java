package io.kestra.plugin.azure.storage.blob;

import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import io.kestra.core.models.property.Property;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.azure.BaseTest;
import io.kestra.plugin.azure.storage.adls.services.DataLakeService;
import org.junit.jupiter.api.AfterEach;

import java.net.URI;
import java.util.ArrayList;

abstract class AbstractTest extends BaseTest {
    private java.util.List<String> directoryToClean = new ArrayList<>();

    protected Upload.Output upload(String dir) throws Exception {
        URI source = upload();

        String out = IdUtils.create();

        Upload upload = Upload.builder()
            .id(AllTest.class.getSimpleName())
            .type(Upload.class.getName())
            .endpoint(Property.of(this.storageEndpoint))
            .connectionString(Property.of(connectionString))
            .container(Property.of(this.container))
            .from(Property.of(source.toString()))
            .name(Property.of(dir + "/" + out + ".yml"))
            .build();

        directoryToClean.add(dir);

        return upload.run(runContext(upload));
    }

    protected List.ListBuilder<?, ?> list() {
        return List.builder()
            .id(AbstractTest.class.getSimpleName())
            .type(List.class.getName())
            .endpoint(Property.of(this.storageEndpoint))
            .connectionString(Property.of(connectionString))
            .container(Property.of(this.container));
    }

    protected DeleteList.DeleteListBuilder<?, ?> deleteDir(String dir) {
        return DeleteList.builder()
            .id(AbstractTest.class.getSimpleName())
            .type(DeleteList.class.getName())
            .endpoint(Property.of(this.storageEndpoint))
            .connectionString(Property.of(connectionString))
            .container(Property.of(this.container))
            .prefix(Property.of(dir));
    }

    @AfterEach
    void cleanup() throws Exception {
        for (String dirName : directoryToClean) {
            deleteDir(dirName);
        }
    }
}
