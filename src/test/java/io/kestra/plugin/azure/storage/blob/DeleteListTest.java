package io.kestra.plugin.azure.storage.blob;

import io.kestra.core.models.property.Property;
import io.kestra.core.utils.IdUtils;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

import org.junit.jupiter.api.Disabled;

@Disabled("Requires Azure Storage credentials not available in CI")
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
            .endpoint(Property.ofValue(this.storageEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .container(Property.ofValue(this.container))
            .prefix(Property.ofValue("/tasks/" + prefix + "/"))
            .concurrent(5)
            .build();

        DeleteList.Output run = task.run(runContext(task));
        assertThat(run.getCount(), is(10L));
        assertThat(run.getSize(), greaterThan(1000L));
    }
}
