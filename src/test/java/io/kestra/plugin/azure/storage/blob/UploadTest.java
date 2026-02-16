package io.kestra.plugin.azure.storage.blob;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

@KestraTest
class UploadTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldHandleFromAsProperty() throws Exception {
        var task = Upload.builder()
            .id("upload-test")
            .type(Upload.class.getName())
            .endpoint(Property.ofValue("https://example.blob.core.windows.net"))
            .container(Property.ofValue("container"))
            .name(Property.ofValue("blob.txt"))
            .from(Property.ofValue(List.of()))
            .build();

        var output = task.run(runContextFactory.of());

        assertThat(output.getBlob(), nullValue());
        assertThat(output.getBlobs(), hasSize(0));
    }
}
