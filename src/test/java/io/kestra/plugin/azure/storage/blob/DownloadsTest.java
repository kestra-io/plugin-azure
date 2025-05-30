package io.kestra.plugin.azure.storage.blob;

import io.kestra.core.models.property.Property;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.azure.storage.blob.abstracts.ActionInterface;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.endsWith;
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
            .endpoint(Property.ofValue(this.storageEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .container(Property.ofValue(this.container))
            .prefix(Property.ofValue("/tasks/" + prefix + "/abs/"))
            .action(Property.ofValue(ActionInterface.Action.DELETE))
            .build();

        Downloads.Output run = task.run(runContext(task));

        assertThat(run.getBlobs().size(), is(2));
        assertThat(run.getBlobs().get(0).getUri().toString(), endsWith(".yml"));
        assertThat(run.getOutputFiles().size(), is(2));

        List list = list().prefix(Property.ofValue("/tasks/" + prefix + "/abs/")).build();
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
            .endpoint(Property.ofValue(this.storageEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .container(Property.ofValue(this.container))
            .prefix(Property.ofValue("/tasks/" + prefix + "/abs-from/"))
            .action(Property.ofValue(ActionInterface.Action.MOVE))
            .moveTo(Copy.CopyObject.builder()
                .container(Property.ofValue(this.container))
                .name(Property.ofValue("/tasks/" + prefix + "/blobs-move"))
                .build()
            )
            .build();

        Downloads.Output run = task.run(runContext(task));

        assertThat(run.getBlobs().size(), is(2));
        assertThat(run.getOutputFiles().size(), is(2));

        List list = list().prefix(Property.ofValue("/tasks/" + prefix + "/blobs-from/")).build();
        List.Output listOutput = list.run(runContext(list));
        assertThat(listOutput.getBlobs().size(), is(0));

        list = list().prefix(Property.ofValue("/tasks/" + prefix + "/blobs-move")).build();
        listOutput = list.run(runContext(list));
        assertThat(listOutput.getBlobs().size(), is(2));
    }
}
