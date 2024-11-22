package io.kestra.plugin.azure.storage.blob;

import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
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
            .endpoint(this.storageEndpoint)
            .connectionString(this.connectionString)
            .container(this.container)
            .from(source.toString())
            .name(dir + "/" + out + ".yml")
            .build();

        directoryToClean.add(dir);

        return upload.run(runContext(upload));
    }

    protected List.ListBuilder<?, ?> list() {
        return List.builder()
            .id(AbstractTest.class.getSimpleName())
            .type(List.class.getName())
            .endpoint(this.storageEndpoint)
            .connectionString(this.connectionString)
            .container(this.container);
    }

    protected DeleteList.DeleteListBuilder<?, ?> deleteDir(String dir) {
        return DeleteList.builder()
            .id(AbstractTest.class.getSimpleName())
            .type(DeleteList.class.getName())
            .endpoint(this.storageEndpoint)
            .connectionString(this.connectionString)
            .container(this.container)
            .prefix(dir);
    }

    @AfterEach
    void cleanup() throws Exception {
        for (String dirName : directoryToClean) {
            deleteDir(dirName);
        }
    }
}
