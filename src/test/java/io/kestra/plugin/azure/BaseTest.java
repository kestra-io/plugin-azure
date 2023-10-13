package io.kestra.plugin.azure;

import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Objects;

@MicronautTest
public abstract class BaseTest {
    @Inject
    protected RunContextFactory runContextFactory;

    @Inject
    protected StorageInterface storageInterface;

    @Value("${kestra.variables.globals.azure.blobs.endpoint}")
    protected String storageEndpoint;


    @Value("${kestra.variables.globals.azure.blobs.connection-string}")
    protected String connectionString;

    @Value("${kestra.variables.globals.azure.blobs.container}")
    protected String container;

    @Inject
    protected static File file() throws URISyntaxException {
        return new File(Objects.requireNonNull(BaseTest.class.getClassLoader()
                .getResource("application.yml"))
            .toURI());
    }

    protected URI upload() throws URISyntaxException, IOException {
        return storageInterface.put(
            null,
            new URI("/" + IdUtils.create() + ".yml"),
            new FileInputStream(file())
        );
    }

    protected URI upload(byte[] content) throws URISyntaxException, IOException {
        return storageInterface.put(
            null,
            new URI("/" + IdUtils.create() + ".yml"),
            new ByteArrayInputStream(content)
        );
    }

    protected RunContext runContext(Task task) {
        return runContext(task, Map.of());
    }

    protected RunContext runContext(Task task, Map<String, Object> vars) {
        return TestsUtils.mockRunContext(
            this.runContextFactory,
            task,
            vars
        );
    }
}
