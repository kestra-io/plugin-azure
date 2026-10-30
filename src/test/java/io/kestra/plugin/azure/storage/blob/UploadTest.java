package io.kestra.plugin.azure.storage.blob;

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.github.tomakehurst.wiremock.WireMockServer;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;

import jakarta.inject.Inject;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class UploadTest {
    private static final String ACCOUNT = "devstoreaccount1";

    @Inject
    private RunContextFactory runContextFactory;

    private WireMockServer server;

    @BeforeEach
    void start() {
        server = new WireMockServer(options().dynamicPort());
        server.start();
    }

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop();
        }
    }

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
    void shouldComputeTargetBlobNameForSingleAndMultiFiles() {
        // Single file uploads
        assertThat(Upload.computeTargetBlobName("data.csv", "source.csv", false), is("data.csv"));
        assertThat(Upload.computeTargetBlobName("incoming/", "file1.csv", false), is("incoming/file1.csv"));
        assertThat(Upload.computeTargetBlobName("incoming", "file1.csv", false), is("incoming"));

        // Multi-file uploads
        assertThat(Upload.computeTargetBlobName("incoming/", "file1.csv", true), is("incoming/file1.csv"));
        assertThat(Upload.computeTargetBlobName("incoming/", "file2.csv", true), is("incoming/file2.csv"));
        assertThat(Upload.computeTargetBlobName("incoming", "file1.csv", true), is("incoming/file1.csv"));
        assertThat(Upload.computeTargetBlobName("incoming", "file2.csv", true), is("incoming/file2.csv"));
        assertThat(Upload.computeTargetBlobName("backup.csv", "file1.csv", true), is("backup.csv/file1.csv"));
        assertThat(Upload.computeTargetBlobName("backup.csv", "file2.csv", true), is("backup.csv/file2.csv"));

        // Edge cases
        assertThat(Upload.computeTargetBlobName("", "file1.csv", true), is("file1.csv"));
        assertThat(Upload.computeTargetBlobName(null, "file1.csv", true), is("file1.csv"));
        assertThat(Upload.computeTargetBlobName("incoming/", "dir\\file1.csv", true), is("incoming/dir/file1.csv"));
        assertThat(Upload.computeTargetBlobName("incoming/", "/file1.csv", true), is("incoming/file1.csv"));
        assertThat(Upload.computeTargetBlobName("incoming/", "nested/file1.csv", true), is("incoming/nested/file1.csv"));
    }

    @Test
    void shouldFailFastOnDuplicateDestinationBlobNamesFromList() {
        var runContext = runContextFactory.of();

        var task = Upload.builder()
            .id("upload-collision-test")
            .type(Upload.class.getName())
            .endpoint(Property.ofValue("https://example.blob.core.windows.net"))
            .container(Property.ofValue("container"))
            .name(Property.ofValue("incoming/"))
            .from(Property.ofValue(List.of(
                "kestra:///taskA/report.csv",
                "kestra:///taskB/report.csv"
            )))
            .build();

        var ex = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));

        assertThat(ex.getMessage(), containsString("Duplicate destination blob 'incoming/report.csv' detected for source 'kestra:///taskB/report.csv'"));
        assertThat(ex.getMessage(), containsString("Multiple files cannot be uploaded to the same blob name"));
    }

    @Test
    void shouldUploadMultipleFilesConsistentlyUnderDirectory() throws Exception {
        var key = Base64.getEncoder().encodeToString("test-account-key".getBytes());
        var runContext = runContextFactory.of();

        File tempFile1 = File.createTempFile("file1", ".csv");
        Files.writeString(tempFile1.toPath(), "col1,col2\nval1,val2\n");
        URI uri1 = runContext.storage().putFile(tempFile1);

        File tempFile2 = File.createTempFile("file2", ".csv");
        Files.writeString(tempFile2.toPath(), "colA,colB\nvalA,valB\n");
        URI uri2 = runContext.storage().putFile(tempFile2);

        String fileName1 = tempFile1.getName();
        String fileName2 = tempFile2.getName();

        server.stubFor(put(urlPathMatching("/" + ACCOUNT + "/my-container/.*"))
            .willReturn(aResponse()
                .withStatus(201)
                .withHeader("ETag", "\"0x1\"")
                .withHeader("Last-Modified", "Tue, 01 Jan 2030 00:00:00 GMT")));

        server.stubFor(head(urlPathMatching("/" + ACCOUNT + "/my-container/.*"))
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Length", "20")
                .withHeader("x-ms-blob-type", "BlockBlob")
                .withHeader("ETag", "\"0x1\"")
                .withHeader("Last-Modified", "Tue, 01 Jan 2030 00:00:00 GMT")));

        var task = Upload.builder()
            .id(IdUtils.create())
            .type(Upload.class.getName())
            .connectionString(Property.ofValue(
                "DefaultEndpointsProtocol=http;AccountName=" + ACCOUNT + ";AccountKey=" + key + ";BlobEndpoint=" + server.baseUrl() + "/" + ACCOUNT
            ))
            .container(Property.ofValue("my-container"))
            .name(Property.ofValue("incoming/"))
            .from(Property.ofValue(List.of(uri1.toString(), uri2.toString())))
            .build();

        var output = task.run(runContext);

        assertThat(output.getBlob(), nullValue());
        assertThat(output.getBlobs(), hasSize(2));
        assertThat(output.getBlobs().get(0).getName(), is("incoming/" + fileName1));
        assertThat(output.getBlobs().get(1).getName(), is("incoming/" + fileName2));

        // Verify the first file is NOT uploaded literally as "incoming/"
        server.verify(0, putRequestedFor(urlPathEqualTo("/" + ACCOUNT + "/my-container/incoming%2F")));
        server.verify(1, putRequestedFor(urlPathEqualTo("/" + ACCOUNT + "/my-container/incoming%2F" + fileName1)));
        server.verify(1, putRequestedFor(urlPathEqualTo("/" + ACCOUNT + "/my-container/incoming%2F" + fileName2)));
    }

    @Test
    void shouldUploadSingleFileWithTrailingSlashDirectory() throws Exception {
        var key = Base64.getEncoder().encodeToString("test-account-key".getBytes());
        var runContext = runContextFactory.of();

        File tempFile = File.createTempFile("single", ".csv");
        Files.writeString(tempFile.toPath(), "data\n");
        URI uri = runContext.storage().putFile(tempFile);
        String fileName = tempFile.getName();

        server.stubFor(put(urlPathMatching("/" + ACCOUNT + "/my-container/.*"))
            .willReturn(aResponse()
                .withStatus(201)
                .withHeader("ETag", "\"0x1\"")
                .withHeader("Last-Modified", "Tue, 01 Jan 2030 00:00:00 GMT")));

        server.stubFor(head(urlPathMatching("/" + ACCOUNT + "/my-container/.*"))
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Length", "5")
                .withHeader("x-ms-blob-type", "BlockBlob")
                .withHeader("ETag", "\"0x1\"")
                .withHeader("Last-Modified", "Tue, 01 Jan 2030 00:00:00 GMT")));

        var task = Upload.builder()
            .id(IdUtils.create())
            .type(Upload.class.getName())
            .connectionString(Property.ofValue(
                "DefaultEndpointsProtocol=http;AccountName=" + ACCOUNT + ";AccountKey=" + key + ";BlobEndpoint=" + server.baseUrl() + "/" + ACCOUNT
            ))
            .container(Property.ofValue("my-container"))
            .name(Property.ofValue("uploads/"))
            .from(Property.ofValue(uri.toString()))
            .build();

        var output = task.run(runContext);

        assertThat(output.getBlob(), notNullValue());
        assertThat(output.getBlob().getName(), is("uploads/" + fileName));
        assertThat(output.getBlobs(), hasSize(1));
        assertThat(output.getBlobs().get(0).getName(), is("uploads/" + fileName));

        // Verify it was uploaded to uploads/single...csv and NOT literally uploads/
        server.verify(0, putRequestedFor(urlPathEqualTo("/" + ACCOUNT + "/my-container/uploads%2F")));
        server.verify(1, putRequestedFor(urlPathEqualTo("/" + ACCOUNT + "/my-container/uploads%2F" + fileName)));
    }

    @Test
    void shouldUploadMultipleFilesFromMap() throws Exception {
        var key = Base64.getEncoder().encodeToString("test-account-key".getBytes());
        var runContext = runContextFactory.of();

        File tempFile1 = File.createTempFile("fileA", ".txt");
        Files.writeString(tempFile1.toPath(), "contentA");
        URI uri1 = runContext.storage().putFile(tempFile1);

        File tempFile2 = File.createTempFile("fileB", ".txt");
        Files.writeString(tempFile2.toPath(), "contentB");
        URI uri2 = runContext.storage().putFile(tempFile2);

        server.stubFor(put(urlPathMatching("/" + ACCOUNT + "/my-container/.*"))
            .willReturn(aResponse()
                .withStatus(201)
                .withHeader("ETag", "\"0x1\"")
                .withHeader("Last-Modified", "Tue, 01 Jan 2030 00:00:00 GMT")));

        server.stubFor(head(urlPathMatching("/" + ACCOUNT + "/my-container/.*"))
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Length", "8")
                .withHeader("x-ms-blob-type", "BlockBlob")
                .withHeader("ETag", "\"0x1\"")
                .withHeader("Last-Modified", "Tue, 01 Jan 2030 00:00:00 GMT")));

        var task = Upload.builder()
            .id(IdUtils.create())
            .type(Upload.class.getName())
            .connectionString(Property.ofValue(
                "DefaultEndpointsProtocol=http;AccountName=" + ACCOUNT + ";AccountKey=" + key + ";BlobEndpoint=" + server.baseUrl() + "/" + ACCOUNT
            ))
            .container(Property.ofValue("my-container"))
            .name(Property.ofValue("mapped/"))
            .from(Property.ofValue(Map.of(
                "custom1.txt", uri1.toString(),
                "custom2.txt", uri2.toString()
            )))
            .build();

        var output = task.run(runContext);

        assertThat(output.getBlob(), nullValue());
        assertThat(output.getBlobs(), hasSize(2));

        server.verify(1, putRequestedFor(urlPathEqualTo("/" + ACCOUNT + "/my-container/mapped%2Fcustom1.txt")));
        server.verify(1, putRequestedFor(urlPathEqualTo("/" + ACCOUNT + "/my-container/mapped%2Fcustom2.txt")));
    }
}
