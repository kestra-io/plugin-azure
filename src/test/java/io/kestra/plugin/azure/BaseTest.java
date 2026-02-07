package io.kestra.plugin.azure;

import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.micronaut.context.annotation.Value;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Objects;

// FIXME Remove once Worker closing has been reworked (Micronaut 4 PR)
//  We need to rebuild the context for each tests as currently Workers can't be closed properly (they keep listening to queues they shouldn't)
@KestraTest(rebuildContext = true)
@EnabledIfEnvironmentVariable(named = "AZURE_CONNECTION_STRING", matches = ".+")
public abstract class BaseTest {
    @Inject
    protected RunContextFactory runContextFactory;

    @Inject
    protected StorageInterface storageInterface;

    @Value("${kestra.variables.globals.azure.blobs.endpoint}")
    protected String storageEndpoint;

    @Value("${kestra.variables.globals.azure.adls.endpoint}")
    protected String adlsEndpoint;

    @Value("${kestra.variables.globals.azure.blobs.connection-string}")
    protected String connectionString;

    @Value("${kestra.variables.globals.azure.blobs.container}")
    protected String container;

    @Value("${kestra.variables.globals.azure.adls.file-system}")
    protected String fileSystem;

    @Inject
    protected static File file(String testFilePath) throws URISyntaxException {
        return new File(Objects.requireNonNull(BaseTest.class.getClassLoader()
                .getResource(testFilePath))
            .toURI());
    }

    protected URI upload() throws URISyntaxException, IOException {
        return storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            new URI("/" + IdUtils.create() + ".yml"),
            new FileInputStream(file("application.yml"))
        );
    }

    protected URI uploadStringFile() throws URISyntaxException, IOException {
        return storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            new URI("/" + IdUtils.create() + ".txt"),
            new FileInputStream(file("testFiles/appendTest.txt"))
        );
    }

    protected URI upload(byte[] content) throws URISyntaxException, IOException {
        return storageInterface.put(
            TenantService.MAIN_TENANT,
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
