package io.kestra.plugin.azure.storage.adls;

import io.kestra.core.models.property.Property;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.azure.storage.blob.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

@EnabledIfEnvironmentVariable(named = "AZURE_CONNECTION_STRING", matches = ".+")
class DeleteFilesTest extends AbstractTest {
    @Test
    void run() throws Exception {
        String prefix = IdUtils.create();

        for (int i = 0; i < 10; i++) {
            upload("adls/azure/" + prefix);
        }

        // all listing
        DeleteFiles task = DeleteFiles.builder()
            .id(DeleteFilesTest.class.getSimpleName())
            .type(List.class.getName())
            .endpoint(Property.ofValue(this.adlsEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .fileSystem(Property.ofValue(this.fileSystem))
            .directoryPath(Property.ofValue("adls/azure/" + prefix + "/"))
            .concurrent(5)
            .build();

        DeleteFiles.Output run = task.run(runContext(task));
        assertThat(run.getCount(), is(10L));
        assertThat(run.getSize(), greaterThan(1000L));
    }
}
