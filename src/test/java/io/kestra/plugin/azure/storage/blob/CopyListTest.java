package io.kestra.plugin.azure.storage.blob;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import io.kestra.core.models.property.Property;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.azure.storage.blob.models.CopyListDestination;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

// Floci Azure returns HTTP 501 for blob copy, same gate as CopyTest
@EnabledIfEnvironmentVariable(named = "AZURE_CONNECTION_STRING", matches = ".+", disabledReason = "Floci Azure returns HTTP 501 for blob copy")
class CopyListTest extends AbstractTest {
    private CopyList.CopyListBuilder<?, ?> task(String prefix, String destPrefix, boolean delete) {
        return CopyList.builder()
            .id(IdUtils.create())
            .type(CopyList.class.getName())
            .endpoint(Property.ofValue(this.storageEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .container(Property.ofValue(this.container))
            .prefix(Property.ofValue(prefix))
            .to(CopyListDestination.builder()
                .container(Property.ofValue(this.container))
                .prefix(Property.ofValue(destPrefix))
                .build())
            .delete(Property.ofValue(delete));
    }

    private int count(String prefix) throws Exception {
        var list = list().prefix(Property.ofValue(prefix)).build();
        return list.run(runContext(list)).getBlobs().size();
    }

    @Test
    void copy() throws Exception {
        var id = IdUtils.create();
        upload("tasks/" + id + "/src");
        upload("tasks/" + id + "/src");

        var task = task("tasks/" + id + "/src/", "tasks/" + id + "/dst/", false).build();
        var output = task.run(runContext(task));

        assertThat(output.getCount(), is(2L));
        assertThat(count("tasks/" + id + "/dst/"), is(2));
        assertThat(count("tasks/" + id + "/src/"), is(2));
    }

    @Test
    void move() throws Exception {
        var id = IdUtils.create();
        upload("tasks/" + id + "/src");
        upload("tasks/" + id + "/src");

        var task = task("tasks/" + id + "/src/", "tasks/" + id + "/dst/", true).concurrent(Property.ofValue(2)).build();
        var output = task.run(runContext(task));

        assertThat(output.getCount(), is(2L));
        assertThat(count("tasks/" + id + "/dst/"), is(2));
        assertThat(count("tasks/" + id + "/src/"), is(0));
    }

    @Test
    void regexpAndNestedPrefix() throws Exception {
        var id = IdUtils.create();
        upload("tasks/" + id + "/src/a/b");
        upload("tasks/" + id + "/src/c");

        var task = task("tasks/" + id + "/src/", "tasks/" + id + "/dst/", false)
            .regexp(Property.ofValue(".*/a/b/.*\\.yml"))
            .build();
        var output = task.run(runContext(task));

        assertThat(output.getCount(), is(1L));
        assertThat(count("tasks/" + id + "/dst/a/b/"), is(1));
        assertThat(count("tasks/" + id + "/dst/c/"), is(0));
    }
}
