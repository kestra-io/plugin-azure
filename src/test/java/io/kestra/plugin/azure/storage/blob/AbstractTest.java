package io.kestra.plugin.azure.storage.blob;

import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.azure.BaseTest;
import io.micronaut.context.annotation.Value;

import java.net.URI;

abstract class AbstractTest extends BaseTest {
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
}
