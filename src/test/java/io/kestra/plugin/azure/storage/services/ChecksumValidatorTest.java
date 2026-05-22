package io.kestra.plugin.azure.storage.services;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.Collections;
import java.util.HexFormat;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class ChecksumValidatorTest {
    @Inject
    private RunContextFactory runContextFactory;

    private RunContext ctx() {
        return runContextFactory.of(Collections.emptyMap());
    }

    private File writeTempFile(byte[] content) throws IOException {
        Path tmp = Files.createTempFile("checksum-test-", ".bin");
        Files.write(tmp, content);
        return tmp.toFile();
    }

    private byte[] md5(byte[] content) throws Exception {
        return MessageDigest.getInstance("MD5").digest(content);
    }

    @Test
    void noOptionsIsNoOp() throws Exception {
        File file = writeTempFile("hello".getBytes());
        assertDoesNotThrow(() -> ChecksumValidator.verify(ctx(), file, null, null, "test"));
    }

    @Test
    void disabledOptionsIsNoOp() throws Exception {
        File file = writeTempFile("hello".getBytes());
        ChecksumValidator.Options opts = ChecksumValidator.Options.builder()
            .validateAgainstServer(false)
            .algorithm(ChecksumValidator.Algorithm.MD5)
            .build();
        assertDoesNotThrow(() -> ChecksumValidator.verify(ctx(), file, new byte[]{1, 2, 3}, opts, "test"));
    }

    @Test
    void serverMd5Match() throws Exception {
        byte[] content = "hello world".getBytes();
        File file = writeTempFile(content);
        ChecksumValidator.Options opts = ChecksumValidator.Options.builder()
            .validateAgainstServer(true)
            .algorithm(ChecksumValidator.Algorithm.MD5)
            .build();
        ChecksumValidator.verify(ctx(), file, md5(content), opts, "test");
    }

    @Test
    void serverMd5Mismatch() throws Exception {
        byte[] content = "hello world".getBytes();
        File file = writeTempFile(content);
        byte[] wrong = md5("different".getBytes());
        ChecksumValidator.Options opts = ChecksumValidator.Options.builder()
            .validateAgainstServer(true)
            .algorithm(ChecksumValidator.Algorithm.MD5)
            .build();
        IOException e = assertThrows(IOException.class,
            () -> ChecksumValidator.verify(ctx(), file, wrong, opts, "test"));
        assertThat(e.getMessage(), containsString("mismatch"));
    }

    @Test
    void missingServerChecksumSkippedByDefault() throws Exception {
        File file = writeTempFile("hello".getBytes());
        ChecksumValidator.Options opts = ChecksumValidator.Options.builder()
            .validateAgainstServer(true)
            .failOnMissingServerChecksum(false)
            .algorithm(ChecksumValidator.Algorithm.MD5)
            .build();
        assertDoesNotThrow(() -> ChecksumValidator.verify(ctx(), file, null, opts, "test"));
    }

    @Test
    void missingServerChecksumFailsWhenStrict() throws Exception {
        File file = writeTempFile("hello".getBytes());
        ChecksumValidator.Options opts = ChecksumValidator.Options.builder()
            .validateAgainstServer(true)
            .failOnMissingServerChecksum(true)
            .algorithm(ChecksumValidator.Algorithm.MD5)
            .build();
        IOException e = assertThrows(IOException.class,
            () -> ChecksumValidator.verify(ctx(), file, null, opts, "myblob"));
        assertThat(e.getMessage(), containsString("myblob"));
    }

    @Test
    void expectedHexMatch() throws Exception {
        byte[] content = "hello".getBytes();
        File file = writeTempFile(content);
        String hex = HexFormat.of().formatHex(md5(content));
        ChecksumValidator.Options opts = ChecksumValidator.Options.builder()
            .expected(hex)
            .algorithm(ChecksumValidator.Algorithm.MD5)
            .build();
        ChecksumValidator.verify(ctx(), file, null, opts, "test");
    }

    @Test
    void expectedBase64Match() throws Exception {
        byte[] content = "hello".getBytes();
        File file = writeTempFile(content);
        String b64 = Base64.getEncoder().encodeToString(md5(content));
        ChecksumValidator.Options opts = ChecksumValidator.Options.builder()
            .expected(b64)
            .algorithm(ChecksumValidator.Algorithm.MD5)
            .build();
        ChecksumValidator.verify(ctx(), file, null, opts, "test");
    }

    @Test
    void expectedTakesPrecedenceOverServer() throws Exception {
        byte[] content = "hello".getBytes();
        File file = writeTempFile(content);
        String hex = HexFormat.of().formatHex(md5(content));
        // Server MD5 is wrong, but expected matches → no error
        ChecksumValidator.Options opts = ChecksumValidator.Options.builder()
            .validateAgainstServer(true)
            .expected(hex)
            .algorithm(ChecksumValidator.Algorithm.MD5)
            .build();
        ChecksumValidator.verify(ctx(), file, md5("nope".getBytes()), opts, "test");
    }

    @Test
    void sha256ExpectedMatch() throws Exception {
        byte[] content = "hello".getBytes();
        File file = writeTempFile(content);
        String hex = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(content));
        ChecksumValidator.Options opts = ChecksumValidator.Options.builder()
            .expected(hex)
            .algorithm(ChecksumValidator.Algorithm.SHA_256)
            .build();
        ChecksumValidator.verify(ctx(), file, null, opts, "test");
    }

    @Test
    void sha256AgainstServerIsRejected() throws Exception {
        File file = writeTempFile("hello".getBytes());
        ChecksumValidator.Options opts = ChecksumValidator.Options.builder()
            .validateAgainstServer(true)
            .algorithm(ChecksumValidator.Algorithm.SHA_256)
            .build();
        assertThrows(IllegalArgumentException.class,
            () -> ChecksumValidator.verify(ctx(), file, new byte[]{1}, opts, "test"));
    }

    @Test
    void malformedExpectedIsRejected() throws Exception {
        File file = writeTempFile("hello".getBytes());
        ChecksumValidator.Options opts = ChecksumValidator.Options.builder()
            .expected("not-a-hash")
            .algorithm(ChecksumValidator.Algorithm.MD5)
            .build();
        assertThrows(IllegalArgumentException.class,
            () -> ChecksumValidator.verify(ctx(), file, null, opts, "test"));
    }
}
