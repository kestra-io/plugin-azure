package io.kestra.plugin.azure.storage.blob;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.models.property.Property;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.azure.shared.storage.blob.models.Blob;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

class UploadTest extends AbstractTest {
    private URI storedFile(String dir, String fileName, String content) throws Exception {
        return storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            new URI("/" + dir + "/" + fileName),
            new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8))
        );
    }

    private Upload.UploadBuilder<?, ?> uploadTask(String name) {
        return Upload.builder()
            .id(IdUtils.create())
            .type(Upload.class.getName())
            .endpoint(Property.ofValue(this.storageEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .container(Property.ofValue(this.container))
            .name(Property.ofValue(name));
    }

    private List<String> blobNames(Upload.Output output) {
        return output.getBlobs().stream().map(Blob::getName).toList();
    }

    @Test
    void shouldHandleFromAsProperty() throws Exception {
        var task = Upload.builder()
            .id(IdUtils.create())
            .type(Upload.class.getName())
            .endpoint(Property.ofValue("https://example.blob.core.windows.net"))
            .container(Property.ofValue("container"))
            .name(Property.ofValue("blob.txt"))
            .from(Property.ofValue(List.of()))
            .build();

        var output = task.run(runContext(task));

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
            .id(IdUtils.create())
            .type(Upload.class.getName())
            .endpoint(Property.ofValue("https://example.blob.core.windows.net"))
            .container(Property.ofValue("container"))
            .name(Property.ofValue("uploads/"))
            .from(Property.ofValue(List.of(
                "kestra:///taskA/report.csv",
                "kestra:///taskB/report.csv"
            )))
            .build();

        var exception = assertThrows(IllegalArgumentException.class, () -> task.run(runContext(task)));
        assertThat(exception.getMessage(), containsString("multiple source URIs resolve to the same destination blob name 'uploads/report.csv'"));
    }

    @Test
    void shouldThrowWhenDuplicateDestinationBlobNamesForRenderedJsonList() {
        var task = Upload.builder()
            .id(IdUtils.create())
            .type(Upload.class.getName())
            .endpoint(Property.ofValue("https://example.blob.core.windows.net"))
            .container(Property.ofValue("container"))
            .name(Property.ofValue("uploads/"))
            .from("[\"kestra:///taskA/report.csv\", \"kestra:///taskB/report.csv\"]")
            .build();

        var exception = assertThrows(IllegalArgumentException.class, () -> task.run(runContext(task)));
        assertThat(exception.getMessage(), containsString("multiple source URIs resolve to the same destination blob name 'uploads/report.csv'"));
    }

    @Test
    void shouldUploadListOfRenderedUrisUnderPrefix() throws Exception {
        var prefix = "uploads-" + IdUtils.create() + "/";
        var dir = IdUtils.create();
        var first = storedFile(dir, "file1.csv", "one");
        var second = storedFile(dir, "file2.csv", "two");

        var task = uploadTask(prefix)
            .from(List.of("{{ first }}", "{{ second }}"))
            .build();
        var output = task.run(runContextFactory.of(Map.of("first", first.toString(), "second", second.toString())));

        assertThat(output.getBlob(), nullValue());
        assertThat(blobNames(output), containsInAnyOrder(prefix + "file1.csv", prefix + "file2.csv"));
    }

    @Test
    void shouldUploadListOfRenderedMapsUnderPrefix() throws Exception {
        var prefix = "uploads-" + IdUtils.create() + "/";
        var dir = IdUtils.create();
        var first = storedFile(dir, "file1.csv", "one");
        var second = storedFile(dir, "file2.csv", "two");

        var task = uploadTask(prefix)
            .from(List.of(Map.of("uri", "{{ first }}"), Map.of("uri", "{{ second }}")))
            .build();
        var output = task.run(runContextFactory.of(Map.of("first", first.toString(), "second", second.toString())));

        assertThat(blobNames(output), containsInAnyOrder(prefix + "file1.csv", prefix + "file2.csv"));
    }

    @Test
    void shouldUploadRenderedSingleMapAsOneItem() throws Exception {
        var blobName = "single-" + IdUtils.create() + ".csv";
        var file = storedFile(IdUtils.create(), "file1.csv", "one");

        var task = uploadTask(blobName)
            .from(Map.of("uri", "{{ file }}"))
            .build();
        var output = task.run(runContextFactory.of(Map.of("file", file.toString())));

        assertThat(output.getBlob().getName(), is(blobName));
        assertThat(output.getBlobs(), hasSize(1));
    }

    @Test
    void shouldUploadOutputFilesMapNamedAfterItsKeys() throws Exception {
        var prefix = "uploads-" + IdUtils.create() + "/";
        var dir = IdUtils.create();
        var first = storedFile(dir, "generated-1", "one");
        var second = storedFile(dir, "generated-2", "two");

        var task = uploadTask(prefix)
            .from(Map.of("file1.txt", "{{ first }}", "file2.txt", "{{ second }}"))
            .build();
        var output = task.run(runContextFactory.of(Map.of("first", first.toString(), "second", second.toString())));

        assertThat(blobNames(output), containsInAnyOrder(prefix + "file1.txt", prefix + "file2.txt"));
    }

    @Test
    void shouldUploadRenderedOutputFilesExpression() throws Exception {
        var prefix = "uploads-" + IdUtils.create() + "/";
        var dir = IdUtils.create();
        var outputFiles = Map.of(
            "file1.txt", storedFile(dir, "generated-1", "one").toString(),
            "file2.txt", storedFile(dir, "generated-2", "two").toString()
        );

        var task = uploadTask(prefix)
            .from("{{ generatedFiles }}")
            .build();
        var output = task.run(runContextFactory.of(Map.of("generatedFiles", outputFiles)));

        assertThat(blobNames(output), containsInAnyOrder(prefix + "file1.txt", prefix + "file2.txt"));
    }

    @Test
    void shouldRejectDuplicateNamesBeforeUploadingAnything() throws Exception {
        var prefix = "uploads-" + IdUtils.create() + "/";
        var first = storedFile(IdUtils.create(), "report.csv", "one");
        var second = storedFile(IdUtils.create(), "report.csv", "two");

        var task = uploadTask(prefix)
            .from(List.of(first.toString(), second.toString()))
            .build();

        var exception = assertThrows(IllegalArgumentException.class, () -> task.run(runContext(task)));
        assertThat(exception.getMessage(), containsString("'" + prefix + "report.csv'"));

        var listTask = list().prefix(Property.ofValue(prefix)).build();
        assertThat(listTask.run(runContext(listTask)).getBlobs(), hasSize(0));
    }

    @Test
    void shouldFailClearlyOnMalformedJson() {
        var task = uploadTask("uploads/")
            .from("[\"kestra:///a/file1.csv\", ")
            .build();

        var exception = assertThrows(IllegalArgumentException.class, () -> task.run(runContext(task)));
        assertThat(exception.getMessage(), containsString("looks like JSON but could not be parsed"));
    }

    @Test
    void jsonDerivedValuesAreNotRenderedAgain() {
        var task = uploadTask("uploads/")
            .from("{{ json }}")
            .build();

        var exception = assertThrows(
            IllegalArgumentException.class,
            () -> task.run(runContextFactory.of(Map.of("json", "[\"kestra:///a/{{ 1+1 }}.csv\"]")))
        );
        assertThat(exception.getMessage(), containsString("{{ 1+1 }}"));
        assertThat(exception.getMessage(), not(containsString("/2.csv")));
    }

    @Test
    void jsonDerivedMapKeyAndUriStayLiteral() throws Exception {
        var prefix = "uploads-" + IdUtils.create() + "/";
        var file = storedFile(IdUtils.create(), "f1", "one");
        var json = "{\"k-{{ 1+1 }}.txt\": \"" + file + "\"}";

        var output = uploadTask(prefix).from("{{ json }}").build()
            .run(runContextFactory.of(Map.of("json", json)));

        assertThat(blobNames(output), contains(prefix + "k-{{ 1+1 }}.txt"));
    }

    @Test
    void jsonDerivedUriInMapIsNotRenderedAgain() {
        var task = uploadTask("uploads/")
            .from("{{ json }}")
            .build();

        var exception = assertThrows(
            Exception.class,
            () -> task.run(runContextFactory.of(Map.of("json", "[{\"uri\": \"kestra:///a/{{ 1+1 }}.csv\"}]")))
        );
        assertThat(String.valueOf(exception.getMessage()), not(containsString("2.csv")));
    }

    @Test
    void yamlProvidedValuesAreRenderedOnce() throws Exception {
        var prefix = "uploads-" + IdUtils.create() + "/";
        var dir = IdUtils.create();
        var first = storedFile(dir, "file1.csv", "one");
        var second = storedFile(dir, "file2.csv", "two");
        var ctx = Map.<String, Object>of("first", first.toString(), "second", second.toString());

        var fromList = uploadTask(prefix).from(List.of("{{ first }}", Map.of("uri", "{{ second }}"))).build()
            .run(runContextFactory.of(ctx));
        assertThat(blobNames(fromList), containsInAnyOrder(prefix + "file1.csv", prefix + "file2.csv"));

        var single = IdUtils.create() + ".csv";
        var fromMap = uploadTask(single).from(Map.of("uri", "{{ first }}")).build()
            .run(runContextFactory.of(ctx));
        assertThat(fromMap.getBlob().getName(), is(single));
    }
}
