package io.kestra.plugin.azure.storage.adls;

import io.kestra.core.models.property.Property;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.azure.BaseTest;
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
    void readWithMatchingExpectedChecksumSucceeds() throws Exception {
        String prefix = IdUtils.create();
        Upload.Output upload = upload("adls/azure/" + prefix);

        Read read = Read.builder()
            .id(ChecksumTest.class.getSimpleName())
            .type(Read.class.getName())
            .endpoint(Property.ofValue(this.adlsEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .fileSystem(Property.ofValue(this.fileSystem))
            .filePath(Property.ofValue(upload.getFile().getName()))
            .expectedChecksum(Property.ofValue(md5OfApplicationYml()))
            .build();

        Read.Output run = read.run(runContext(read));
        assertThat(run.getFile().getUri(), is(notNullValue()));
    }

    @Test
    void readWithMismatchedExpectedChecksumFails() throws Exception {
        String prefix = IdUtils.create();
        Upload.Output upload = upload("adls/azure/" + prefix);

        Read read = Read.builder()
            .id(ChecksumTest.class.getSimpleName())
            .type(Read.class.getName())
            .endpoint(Property.ofValue(this.adlsEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .fileSystem(Property.ofValue(this.fileSystem))
            .filePath(Property.ofValue(upload.getFile().getName()))
            .expectedChecksum(Property.ofValue("00000000000000000000000000000000"))
            .build();

        IOException e = assertThrows(IOException.class,
            () -> read.run(runContext(read)));
        assertThat(e.getMessage(), containsString("Checksum mismatch"));
    }

    @Test
    void readValidateAgainstServerSkipsWhenNoServerMd5() throws Exception {
        String prefix = IdUtils.create();
        Upload.Output upload = upload("adls/azure/" + prefix);

        Read read = Read.builder()
            .id(ChecksumTest.class.getSimpleName())
            .type(Read.class.getName())
            .endpoint(Property.ofValue(this.adlsEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .fileSystem(Property.ofValue(this.fileSystem))
            .filePath(Property.ofValue(upload.getFile().getName()))
            .validateChecksum(Property.ofValue(true))
            .build();

        assertDoesNotThrow(() -> read.run(runContext(read)));
    }

    @Test
    void readsValidateChecksumAcrossFiles() throws Exception {
        String prefix = IdUtils.create();
        upload("adls/azure/" + prefix);
        upload("adls/azure/" + prefix);

        Reads task = Reads.builder()
            .id(ChecksumTest.class.getSimpleName())
            .type(Reads.class.getName())
            .endpoint(Property.ofValue(this.adlsEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .fileSystem(Property.ofValue(this.fileSystem))
            .directoryPath(Property.ofValue("adls/azure/" + prefix + "/"))
            .validateChecksum(Property.ofValue(true))
            .build();

        Reads.Output run = task.run(runContext(task));
        assertThat(run.getFiles().size(), is(2));
        assertThat(run.getOutputFiles().size(), is(2));
    }

    @Test
    void uploadValidateChecksumStoresHashMatchingLocal() throws Exception {
        String prefix = IdUtils.create();
        Upload.Output upload = upload("adls/azure/" + prefix, true);

        Read read = Read.builder()
            .id(ChecksumTest.class.getSimpleName())
            .type(Read.class.getName())
            .endpoint(Property.ofValue(this.adlsEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .fileSystem(Property.ofValue(this.fileSystem))
            .filePath(Property.ofValue(upload.getFile().getName()))
            .expectedChecksum(Property.ofValue(md5OfApplicationYml()))
            .build();

        Read.Output run = read.run(runContext(read));
        assertThat(run.getFile().getUri(), is(notNullValue()));
    }

    @Test
    void uploadValidateChecksumRoundTripsCleanly() throws Exception {
        String prefix = IdUtils.create();
        Upload.Output upload = upload("adls/azure/" + prefix, true);

        Read read = Read.builder()
            .id(ChecksumTest.class.getSimpleName())
            .type(Read.class.getName())
            .endpoint(Property.ofValue(this.adlsEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .fileSystem(Property.ofValue(this.fileSystem))
            .filePath(Property.ofValue(upload.getFile().getName()))
            .validateChecksum(Property.ofValue(true))
            .build();

        assertDoesNotThrow(() -> read.run(runContext(read)));
    }

    @Test
    void readsStrictMissingChecksumFails() throws Exception {
        String prefix = IdUtils.create();
        upload("adls/azure/" + prefix);

        Reads task = Reads.builder()
            .id(ChecksumTest.class.getSimpleName())
            .type(Reads.class.getName())
            .endpoint(Property.ofValue(this.adlsEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .fileSystem(Property.ofValue(this.fileSystem))
            .directoryPath(Property.ofValue("adls/azure/" + prefix + "/"))
            .validateChecksum(Property.ofValue(true))
            .failOnMissingChecksum(Property.ofValue(true))
            .build();

        assertThrows(IOException.class, () -> task.run(runContext(task)));
    }
}
