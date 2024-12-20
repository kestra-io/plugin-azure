package io.kestra.plugin.azure.storage.blob;

import io.kestra.core.models.property.Property;
import io.kestra.core.utils.IdUtils;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class CopyTest extends AbstractTest {
    void run(Boolean delete) throws Exception {
        String prefix = IdUtils.create();

        Upload.Output upload = upload("/tasks/" + prefix + "/" + IdUtils.create() + "/sub");
        Upload.Output move = upload("/tasks/" + prefix + "/" + IdUtils.create() + "/sub");

        // copy
        Copy task = Copy.builder()
            .id(CopyTest.class.getSimpleName())
            .type(List.class.getName())
            .endpoint(Property.of(this.storageEndpoint))
            .connectionString(Property.of(connectionString))
            .from(Copy.CopyObject.builder()
                .container(Property.of(this.container))
                .name(Property.of(upload.getBlob().getName()))
                .build()
            )
            .to(Copy.CopyObject.builder()
                .container(Property.of(this.container))
                .name(Property.of(move.getBlob().getName()))
                .build()
            )
            .delete(Property.of(delete))
            .build();

        Copy.Output run = task.run(runContext(task));
        assertThat(run.getBlob().getName(), is(move.getBlob().getName()));

        // list
        List list = list().prefix(Property.of(move.getBlob().getName())).build();

        List.Output listOutput = list.run(runContext(list));
        assertThat(listOutput.getBlobs().size(), is(1));

        // original is here
        list = list().prefix(Property.of(upload.getBlob().getName())).build();

        listOutput = list.run(runContext(list));
        assertThat(listOutput.getBlobs().size(), is(delete ? 0 : 1));
    }

    @Test
    void run() throws Exception {
        this.run(false);
    }

    @Test
    void delete() throws Exception {
        this.run(true);
    }
}
