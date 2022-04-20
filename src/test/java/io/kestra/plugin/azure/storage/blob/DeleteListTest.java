package io.kestra.plugin.azure.storage.blob;

import io.kestra.core.utils.IdUtils;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

class DeleteListTest extends AbstractTest {
    @Test
    void run() throws Exception {
        String prefix = IdUtils.create();

        for (int i = 0; i < 10; i++) {
            upload("/tasks/" + prefix + "/");
        }

        // all listing
        DeleteList task = DeleteList.builder()
            .id(DeleteListTest.class.getSimpleName())
            .type(List.class.getName())
            .endpoint(this.endpoint)
            .connectionString(this.connectionString)
            .container(this.container)
            .prefix("/tasks/" + prefix + "/")
            .concurrent(5)
            .build();

        DeleteList.Output run = task.run(runContext(task));
        assertThat(run.getCount(), is(10L));
        assertThat(run.getSize(), greaterThan(1000L));
    }
}
