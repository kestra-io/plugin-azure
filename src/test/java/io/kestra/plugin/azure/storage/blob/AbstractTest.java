package io.kestra.plugin.azure.storage.blob;

import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.azure.BaseTest;
import io.micronaut.context.annotation.Value;

import java.io.FileInputStream;
import java.net.URI;

abstract class AbstractTest extends BaseTest {
    @Value("${kestra.variables.globals.azure.blobs.endpoint}")
    protected String endpoint;

    @Value("${kestra.variables.globals.azure.blobs.connection-string}")
    protected String connectionString;

    @Value("${kestra.variables.globals.azure.blobs.container}")
    protected String container;

    protected Upload.Output upload(String dir) throws Exception {
        URI source = upload();

        String out = IdUtils.create();

        Upload upload = Upload.builder()
            .id(AllTest.class.getSimpleName())
            .type(Upload.class.getName())
            .endpoint(this.endpoint)
            .connectionString(this.connectionString)
            .container(this.container)
            .from(source.toString())
            .name(dir + "/" + out)
            .build();

        return upload.run(runContext(upload));
    }

    protected List.ListBuilder<?, ?> list() {
        return List.builder()
            .id(AbstractTest.class.getSimpleName())
            .type(List.class.getName())
            .endpoint(this.endpoint)
            .connectionString(this.connectionString)
            .container(this.container);
    }
}
