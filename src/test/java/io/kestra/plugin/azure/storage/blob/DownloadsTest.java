package io.kestra.plugin.azure.storage.blob;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import io.kestra.core.models.property.Property;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.azure.shared.storage.blob.abstracts.ListInterface;
import io.kestra.plugin.azure.storage.blob.abstracts.ActionInterface;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
    @EnabledIfEnvironmentVariable(named = "AZURE_CONNECTION_STRING", matches = ".+", disabledReason = "Floci Azure returns HTTP 501 for blob copyFromUrl, which move uses")
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
            .moveTo(
                Copy.CopyObject.builder()
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

    @Test
    void defaultAction() throws Exception {
        var prefix = IdUtils.create();

        upload("/tasks/" + prefix + "/abs-default");
        upload("/tasks/" + prefix + "/abs-default");

        var task = Downloads.builder()
            .id(IdUtils.create())
            .type(Downloads.class.getName())
            .endpoint(Property.ofValue(this.storageEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .container(Property.ofValue(this.container))
            .prefix(Property.ofValue("/tasks/" + prefix + "/abs-default/"))
            .build();

        var run = task.run(runContext(task));

        assertThat(run.getBlobs().size(), is(2));
        assertThat(run.getOutputFiles().size(), is(2));

        var list = list().prefix(Property.ofValue("/tasks/" + prefix + "/abs-default/")).build();
        var listOutput = list.run(runContext(list));
        assertThat(listOutput.getBlobs().size(), is(2));
    }

    @Test
    void filesFilterWithDelimiter() throws Exception {
        var prefix = IdUtils.create();

        upload("/tasks/" + prefix + "/filter-test");
        update("/tasks/" + prefix + "/filter-test/virtual-dir/");

        var filesTask = Downloads.builder()
            .id(IdUtils.create())
            .type(Downloads.class.getName())
            .endpoint(Property.ofValue(this.storageEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .container(Property.ofValue(this.container))
            .prefix(Property.ofValue("/tasks/" + prefix + "/filter-test/"))
            .delimiter(Property.ofValue("/"))
            .filter(Property.ofValue(ListInterface.Filter.FILES))
            .build();

        var filesRun = filesTask.run(runContext(filesTask));
        assertThat(filesRun.getBlobs().size(), is(1));
        assertThat(filesRun.getBlobs().getFirst().getName().endsWith(".yml"), is(true));
        assertThat(filesRun.getOutputFiles().size(), is(1));
    }

    @Test
    void customFilterWithoutDelimiter() throws Exception {
        var prefix = IdUtils.create();

        update("/tasks/" + prefix + "/filter-custom/file.yml");
        update("/tasks/" + prefix + "/filter-custom/dir/");

        var filesTask = Downloads.builder()
            .id(IdUtils.create())
            .type(Downloads.class.getName())
            .endpoint(Property.ofValue(this.storageEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .container(Property.ofValue(this.container))
            .prefix(Property.ofValue("/tasks/" + prefix + "/filter-custom/"))
            .filter(Property.ofValue(ListInterface.Filter.FILES))
            .build();

        var filesRun = filesTask.run(runContext(filesTask));
        assertThat(filesRun.getBlobs().size(), is(2));

        var dirTask = Downloads.builder()
            .id(IdUtils.create())
            .type(Downloads.class.getName())
            .endpoint(Property.ofValue(this.storageEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .container(Property.ofValue(this.container))
            .prefix(Property.ofValue("/tasks/" + prefix + "/filter-custom/"))
            .filter(Property.ofValue(ListInterface.Filter.DIRECTORY))
            .build();

        var dirRun = dirTask.run(runContext(dirTask));
        assertThat(dirRun.getBlobs().isEmpty(), is(true));
        assertThat(dirRun.getOutputFiles().isEmpty(), is(true));
    }

    @Test
    void directoryFilterWithDelimiter() {
        var task = Downloads.builder()
            .id(IdUtils.create())
            .type(Downloads.class.getName())
            .endpoint(Property.ofValue(this.storageEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .container(Property.ofValue(this.container))
            .prefix(Property.ofValue("/tasks/test/"))
            .delimiter(Property.ofValue("/"))
            .filter(Property.ofValue(ListInterface.Filter.DIRECTORY))
            .build();

        assertThrows(IllegalArgumentException.class, () -> task.run(runContext(task)));
    }
}
