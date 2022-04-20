package io.kestra.plugin.azure.storage.blob;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

@MicronautTest
abstract class AbstractTest {
    @Inject
    protected RunContextFactory runContextFactory;

    @Inject
    protected StorageInterface storageInterface;

    @Value("${kestra.variables.globals.azure.blobs.endpoint}")
    protected String endpoint;

    @Value("${kestra.variables.globals.azure.blobs.connection-string}")
    protected String connectionString;

    @Value("${kestra.variables.globals.azure.blobs.container}")
    protected String container;

    @Inject
    protected static File file() throws URISyntaxException {
        return new File(Objects.requireNonNull(AbstractTest.class.getClassLoader()
                .getResource("application.yml"))
            .toURI());
    }


    protected Upload.Output upload(String dir) throws Exception {
        URI source = storageInterface.put(
            new URI("/" + IdUtils.create()),
            new FileInputStream(file())
        );

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

    protected RunContext runContext(Task task) {
        return TestsUtils.mockRunContext(
            this.runContextFactory,
            task,
            ImmutableMap.of()
        );
    }
}
