package io.kestra.plugin.azure.storage.blob;

import io.kestra.core.models.property.Property;
import io.kestra.core.utils.IdUtils;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.FileReader;
import java.nio.charset.StandardCharsets;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

class SharedAccessTest extends AbstractTest {
    @Test
    void run() throws Exception {
        String prefix = IdUtils.create();

        Upload.Output upload = upload("/tasks/" + prefix + "/" + IdUtils.create() + "/sub");

        SharedAccess task = SharedAccess.builder()
            .id(CopyTest.class.getSimpleName())
            .type(List.class.getName())
            .endpoint(Property.of(this.storageEndpoint))
            .connectionString(Property.of(connectionString))
            .container(Property.of(upload.getBlob().getContainer()))
            .name(Property.of(upload.getBlob().getName()))
            .expirationDate(new Property<>("{{ now() | dateAdd(1, 'DAYS')  }}"))
            .permissions(Set.of(SharedAccess.Permission.READ))
            .build();
        SharedAccess.Output run = task.run(runContext(task));

        assertThat(run.getUri(), is(notNullValue()));

        String downloaded = IOUtils.toString(run.getUri(), StandardCharsets.UTF_8);
        String resource = IOUtils.toString(new FileReader(file("application.yml")));
        assertThat(downloaded, is(resource));
    }
}
