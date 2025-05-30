package io.kestra.plugin.azure.storage.adls;

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
        final String prefix = IdUtils.create();
        final String path = "adls/azure/tasks/" + prefix + "/" + IdUtils.create() + "/sub";

        Upload.Output upload = upload(path);

        SharedAccess task = SharedAccess.builder()
            .id(SharedAccess.class.getSimpleName())
            .type(List.class.getName())
            .endpoint(Property.ofValue(this.adlsEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .fileSystem(Property.ofValue(this.fileSystem))
            .filePath(Property.ofValue(upload.getFile().getName()))
            .expirationDate(Property.ofExpression(" {{ now() | dateAdd(1, 'DAYS')  }}"))
            .permissions(Set.of(SharedAccess.Permission.READ))
            .build();
        SharedAccess.Output run = task.run(runContext(task));

        assertThat(run.getUri(), is(notNullValue()));

        String downloaded = IOUtils.toString(run.getUri(), StandardCharsets.UTF_8);
        String resource = IOUtils.toString(new FileReader(file("application.yml")));
        assertThat(downloaded, is(resource));
    }
}
