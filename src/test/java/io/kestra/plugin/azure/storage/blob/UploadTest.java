package io.kestra.plugin.azure.storage.blob;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;

@KestraTest
@MicronautTest
@EnabledIfEnvironmentVariable(named = "AZURE_CONNECTION_STRING", matches = ".+")
class UploadTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Value("${kestra.variables.globals.azure.blobs.connection-string}")
    private String connectionString;

    @Value("${kestra.variables.globals.azure.blobs.endpoint}")
    private String endpoint;

    private String container;

    @BeforeEach
    void setUp() {
        this.container = "test-upload-" + UUID.randomUUID().toString().substring(0, 8);
        BlobServiceClient serviceClient = new BlobServiceClientBuilder()
                .connectionString(connectionString)
                .buildClient();

        BlobContainerClient containerClient = serviceClient.createBlobContainer(container);
    }

    @Test
    void testSingleFileUpload() throws Exception {
        RunContext runContext = runContextFactory.of();
        String fileContent = "test content for single file upload";
        URI fileUri = storageInterface.put(
                null,
                null,
                new URI("/test-file.txt"),
                new ByteArrayInputStream(fileContent.getBytes(StandardCharsets.UTF_8))
        );

        Upload upload = Upload.builder()
                .endpoint(Property.of(endpoint))
                .connectionString(Property.of(connectionString))
                .container(Property.of(container))
                .name(Property.of("uploaded-file.txt"))
                .from(Property.of(fileUri.toString()))
                .build();

        Upload.Output output = upload.run(runContext);
        assertThat(output.getBlob(), notNullValue());
        assertThat(output.getBlob().getName(), is("uploaded-file.txt"));
        assertThat(output.getBlobs(), hasSize(1));
        assertThat(output.getBlobs().get(0).getName(), is("uploaded-file.txt"));
    }

    @Test
    void testDirectoryUpload() throws Exception {
        RunContext runContext = runContextFactory.of();
        String dir = "/test-dir/";

        storageInterface.put(
                null,
                null,
                new URI(dir + "file1.txt"),
                new ByteArrayInputStream("content 1".getBytes(StandardCharsets.UTF_8))
        );

        storageInterface.put(
                null,
                null,
                new URI(dir + "file2.txt"),
                new ByteArrayInputStream("content 2".getBytes(StandardCharsets.UTF_8))
        );

        storageInterface.put(
                null,
                null,
                new URI(dir + "file3.txt"),
                new ByteArrayInputStream("content 3".getBytes(StandardCharsets.UTF_8))
        );

        Upload upload = Upload.builder()
                .endpoint(Property.of(endpoint))
                .connectionString(Property.of(connectionString))
                .container(Property.of(container))
                .name(Property.of("uploads/"))
                .from(Property.of("kestra://" + dir))
                .build();

        Upload.Output output = upload.run(runContext);

        assertThat(output.getBlob(), nullValue());
        assertThat(output.getBlobs(), hasSize(3));

        assertThat(output.getBlobs().stream()
                .map(blob -> blob.getName())
                .toList(),
                containsInAnyOrder(
                        "uploads/file1.txt",
                        "uploads/file2.txt",
                        "uploads/file3.txt"
                )
        );
    }

    @Test
    void testDirectoryUploadWithoutBaseName() throws Exception {
        RunContext runContext = runContextFactory.of();

        String dir = "/test-dir2/";

        storageInterface.put(
                null,
                null,
                new URI(dir + "file1.txt"),
                new ByteArrayInputStream("content 1".getBytes(StandardCharsets.UTF_8))
        );

        Upload upload = Upload.builder()
                .endpoint(Property.of(endpoint))
                .connectionString(Property.of(connectionString))
                .container(Property.of(container))
                .name(Property.of(""))
                .from(Property.of("kestra://" + dir))
                .build();

        Upload.Output output = upload.run(runContext);

        assertThat(output.getBlobs(), hasSize(1));
        assertThat(output.getBlobs().get(0).getName(), is("file1.txt"));
    }
}
