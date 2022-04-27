package io.kestra.plugin.azure.storage.blob;

import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.azure.storage.blob.abstracts.ActionInterface;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class DownloadsTest extends AbstractTest {
    @Test
    void delete() throws Exception {
        String prefix = IdUtils.create();

        upload("/tasks/" + prefix + "/abs");
        upload("/tasks/" + prefix + "/abs");

        Downloads task = Downloads.builder()
            .id(DownloadsTest.class.getSimpleName())
            .type(Downloads.class.getName())
            .endpoint(this.endpoint)
            .connectionString(this.connectionString)
            .container(this.container)
            .prefix("/tasks/" + prefix + "/abs/")
            .action(ActionInterface.Action.DELETE)
            .build();

        List.Output run = task.run(runContext(task));

        assertThat(run.getBlobs().size(), is(2));

        List list = list().prefix("/tasks/" + prefix + "/abs/").build();
        List.Output listOutput = list.run(runContext(list));
        assertThat(listOutput.getBlobs().size(), is(0));
    }

    @Test
    void move() throws Exception {
        String prefix = IdUtils.create();

        upload("/tasks/" + prefix + "/abs-from");
        upload("/tasks/" + prefix + "/abs-from");

        Downloads task = Downloads.builder()
            .id(DownloadsTest.class.getSimpleName())
            .type(Downloads.class.getName())
            .type(Downloads.class.getName())
            .endpoint(this.endpoint)
            .connectionString(this.connectionString)
            .container(this.container)
            .prefix("/tasks/" + prefix + "/abs-from/")
            .action(ActionInterface.Action.MOVE)
            .moveTo(Copy.CopyObject.builder()
                .container(this.container)
                .name("/tasks/" + prefix + "/s3-move")
                .build()
            )
            .build();

        List.Output run = task.run(runContext(task));

        assertThat(run.getBlobs().size(), is(2));

        List list = list().prefix("/tasks/" + prefix + "/s3-from/").build();
        List.Output listOutput = list.run(runContext(list));
        assertThat(listOutput.getBlobs().size(), is(0));

        list = list().prefix("/tasks/" + prefix + "/s3-move").build();
        listOutput = list.run(runContext(list));
        assertThat(listOutput.getBlobs().size(), is(2));
    }
}
