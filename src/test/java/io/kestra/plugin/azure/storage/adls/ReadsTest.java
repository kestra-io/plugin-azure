package io.kestra.plugin.azure.storage.adls;

import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.azure.storage.blob.List;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class ReadsTest extends AbstractTest {
    @Test
    void run() throws Exception {
        String prefix = IdUtils.create();

        upload("adls/azure/" + prefix);
        upload("adls/azure/" + prefix);

        // all listing
        Reads task = Reads.builder()
            .id(ReadsTest.class.getSimpleName())
            .type(List.class.getName())
            .endpoint(this.adlsEndpoint)
            .connectionString(this.connectionString)
            .fileSystem(this.fileSystem)
            .directoryPath("adls/azure/" + prefix + "/")
            .build();

        Reads.Output run = task.run(runContext(task));

        assertThat(run.getFiles().size(), is(2));
        assertThat(run.getFiles().getFirst().getName(), endsWith(".yml"));
        assertThat(run.getFiles().getLast().getName(), endsWith(".yml"));

        assertThat(run.getFiles().getFirst().getName(), startsWith("adls/azure/" + prefix));
        assertThat(run.getFiles().getLast().getName(), startsWith("adls/azure/" + prefix));

        assertThat(run.getOutputFiles().size(), is(2));
    }
}
