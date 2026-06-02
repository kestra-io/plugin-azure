package io.kestra.plugin.azure.storage.blob;

import io.kestra.core.models.property.Property;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.azure.BaseTest;
import io.kestra.plugin.azure.storage.blob.abstracts.ActionInterface;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.util.HexFormat;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ChecksumTest extends AbstractTest {
    private String md5OfApplicationYml() throws Exception {
        byte[] content = Files.readAllBytes(BaseTest.file("application.yml").toPath());
        byte[] digest = MessageDigest.getInstance("MD5").digest(content);
        return HexFormat.of().formatHex(digest);
    }

    @Test
    void downloadWithMatchingExpectedChecksumSucceeds() throws Exception {
        String prefix = IdUtils.create();
        Upload.Output upload = upload("tasks/azure/" + prefix);

        Download download = Download.builder()
            .id(ChecksumTest.class.getSimpleName())
            .type(Download.class.getName())
            .endpoint(Property.ofValue(this.storageEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .container(Property.ofValue(this.container))
            .name(Property.ofValue(upload.getBlob().getName()))
            .expectedChecksum(Property.ofValue(md5OfApplicationYml()))
            .build();

        Download.Output run = download.run(runContext(download));
        assertThat(run.getBlob().getUri(), is(notNullValue()));
    }

    @Test
    void downloadWithMismatchedExpectedChecksumFails() throws Exception {
        String prefix = IdUtils.create();
        Upload.Output upload = upload("tasks/azure/" + prefix);

        Download download = Download.builder()
            .id(ChecksumTest.class.getSimpleName())
            .type(Download.class.getName())
            .endpoint(Property.ofValue(this.storageEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .container(Property.ofValue(this.container))
            .name(Property.ofValue(upload.getBlob().getName()))
            .expectedChecksum(Property.ofValue("00000000000000000000000000000000"))
            .build();

        IOException e = assertThrows(IOException.class,
            () -> download.run(runContext(download)));
        assertThat(e.getMessage(), containsString("Checksum mismatch"));
    }

    @Test
    void downloadValidateAgainstServerSkipsWhenNoServerMd5() throws Exception {
        String prefix = IdUtils.create();
        Upload.Output upload = upload("tasks/azure/" + prefix);

        // Upload.run streams the file via blobClient.upload(is, true) which does not
        // set Content-MD5. We assert the lenient mode skips silently.
        Download download = Download.builder()
            .id(ChecksumTest.class.getSimpleName())
            .type(Download.class.getName())
            .endpoint(Property.ofValue(this.storageEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .container(Property.ofValue(this.container))
            .name(Property.ofValue(upload.getBlob().getName()))
            .validateChecksum(Property.ofValue(true))
            .build();

        assertDoesNotThrow(() -> download.run(runContext(download)));
    }

    @Test
    void downloadValidateAgainstServerStrictFailsWhenNoServerMd5() throws Exception {
        String prefix = IdUtils.create();
        Upload.Output upload = upload("tasks/azure/" + prefix);

        Download download = Download.builder()
            .id(ChecksumTest.class.getSimpleName())
            .type(Download.class.getName())
            .endpoint(Property.ofValue(this.storageEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .container(Property.ofValue(this.container))
            .name(Property.ofValue(upload.getBlob().getName()))
            .validateChecksum(Property.ofValue(true))
            .failOnMissingChecksum(Property.ofValue(true))
            .build();

        IOException e = assertThrows(IOException.class,
            () -> download.run(runContext(download)));
        assertThat(e.getMessage(), containsString("No Content-MD5"));
    }

    @Test
    void downloadsValidateChecksumSkipsCleanlyAcrossFiles() throws Exception {
        String prefix = IdUtils.create();
        upload("tasks/azure/" + prefix);
        upload("tasks/azure/" + prefix);

        Downloads task = Downloads.builder()
            .id(ChecksumTest.class.getSimpleName())
            .type(Downloads.class.getName())
            .endpoint(Property.ofValue(this.storageEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .container(Property.ofValue(this.container))
            .prefix(Property.ofValue("tasks/azure/" + prefix + "/"))
            .action(Property.ofValue(ActionInterface.Action.NONE))
            .validateChecksum(Property.ofValue(true))
            .build();

        Downloads.Output run = task.run(runContext(task));
        assertThat(run.getBlobs().size(), is(2));
        assertThat(run.getOutputFiles().size(), is(2));
    }


    @Test
    void uploadValidateChecksumStoresHashMatchingLocal() throws Exception {
        String prefix = IdUtils.create();
        Upload.Output upload = upload("tasks/azure/" + prefix, true);

        Download download = Download.builder()
            .id(ChecksumTest.class.getSimpleName())
            .type(Download.class.getName())
            .endpoint(Property.ofValue(this.storageEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .container(Property.ofValue(this.container))
            .name(Property.ofValue(upload.getBlob().getName()))
            .expectedChecksum(Property.ofValue(md5OfApplicationYml()))
            .build();

        Download.Output run = download.run(runContext(download));
        assertThat(run.getBlob().getUri(), is(notNullValue()));
    }

    @Test
    void downloadsStrictMissingChecksumFails() throws Exception {
        String prefix = IdUtils.create();
        upload("tasks/azure/" + prefix);

        Downloads task = Downloads.builder()
            .id(ChecksumTest.class.getSimpleName())
            .type(Downloads.class.getName())
            .endpoint(Property.ofValue(this.storageEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .container(Property.ofValue(this.container))
            .prefix(Property.ofValue("tasks/azure/" + prefix + "/"))
            .action(Property.ofValue(ActionInterface.Action.NONE))
            .validateChecksum(Property.ofValue(true))
            .failOnMissingChecksum(Property.ofValue(true))
            .build();

        assertThrows(IOException.class, () -> task.run(runContext(task)));
    }
}
