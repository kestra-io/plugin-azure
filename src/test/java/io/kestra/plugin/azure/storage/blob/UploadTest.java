package io.kestra.plugin.azure.storage.blob;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

    @Test
    void shouldResolveTargetBlobNameForSingleFile() {
        assertThat(Upload.resolveTargetBlobName("data.csv", "report.csv", false), is("data.csv"));
        assertThat(Upload.resolveTargetBlobName("uploads/", "report.csv", false), is("uploads/report.csv"));
        assertThat(Upload.resolveTargetBlobName("", "report.csv", false), is("report.csv"));
        assertThat(Upload.resolveTargetBlobName(null, "report.csv", false), is("report.csv"));
    }

    @Test
    void shouldResolveTargetBlobNameForMultipleFiles() {
        assertThat(Upload.resolveTargetBlobName("uploads/", "file1.csv", true), is("uploads/file1.csv"));
        assertThat(Upload.resolveTargetBlobName("uploads/", "file2.csv", true), is("uploads/file2.csv"));
        assertThat(Upload.resolveTargetBlobName("backup", "file1.csv", true), is("backup/file1.csv"));
        assertThat(Upload.resolveTargetBlobName("", "file1.csv", true), is("file1.csv"));
        assertThat(Upload.resolveTargetBlobName(null, "file1.csv", true), is("file1.csv"));
    }

    @Test
    void shouldThrowWhenDuplicateDestinationBlobNames() {
        var task = Upload.builder()
            .id("upload-collision-test")
            .type(Upload.class.getName())
            .endpoint(Property.ofValue("https://example.blob.core.windows.net"))
            .container(Property.ofValue("container"))
            .name(Property.ofValue("uploads/"))
            .from(Property.ofValue(List.of(
                "kestra:///taskA/report.csv",
                "kestra:///taskB/report.csv"
            )))
            .build();

        var exception = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));
        assertThat(exception.getMessage(), containsString("multiple source URIs resolve to the same destination blob name 'uploads/report.csv'"));
    }
}
