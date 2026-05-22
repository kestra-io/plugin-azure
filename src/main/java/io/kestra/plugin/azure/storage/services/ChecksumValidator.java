package io.kestra.plugin.azure.storage.services;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import lombok.Builder;
import lombok.Value;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.HexFormat;

public final class ChecksumValidator {
    private ChecksumValidator() {
    }

    public enum Algorithm {
        MD5("MD5"),
        SHA_256("SHA-256");

        private final String javaName;

        Algorithm(String javaName) {
            this.javaName = javaName;
        }

        public String javaName() {
            return javaName;
        }
    }

    @Value
    @Builder
    public static class Options {
        boolean validateAgainstServer;
        boolean failOnMissingServerChecksum;
        String expected;
        Algorithm algorithm;

        public boolean enabled() {
            return validateAgainstServer || expected != null;
        }
    }

    public static Options resolve(
        RunContext runContext,
        Property<Boolean> validateChecksum,
        Property<Boolean> failOnMissingChecksum,
        Property<String> expectedChecksum,
        Property<Algorithm> checksumAlgorithm
    ) throws IllegalVariableEvaluationException {
        boolean rValidate = runContext.render(validateChecksum).as(Boolean.class).orElse(false);
        boolean rFailOnMissing = runContext.render(failOnMissingChecksum).as(Boolean.class).orElse(false);
        String rExpected = runContext.render(expectedChecksum).as(String.class).orElse(null);
        Algorithm rAlgorithm = runContext.render(checksumAlgorithm).as(Algorithm.class).orElse(Algorithm.MD5);

        return Options.builder()
            .validateAgainstServer(rValidate)
            .failOnMissingServerChecksum(rFailOnMissing)
            .expected(rExpected)
            .algorithm(rAlgorithm)
            .build();
    }

    /**
     * Validates the local file's checksum against either a user-supplied expected value
     * or the server-stored Content-MD5. User-supplied {@code expected} takes precedence.
     *
     * @param serverContentMd5 server-stored MD5 bytes, may be null
     */
    public static void verify(
        RunContext runContext,
        File file,
        byte[] serverContentMd5,
        Options options,
        String resourceName
    ) throws IOException {
        if (options == null || !options.enabled()) {
            return;
        }

        Logger logger = runContext.logger();
        Algorithm algorithm = options.getAlgorithm() != null ? options.getAlgorithm() : Algorithm.MD5;

        String expectedHex;
        String comparisonSource;
        if (options.getExpected() != null) {
            expectedHex = normalizeExpected(options.getExpected(), algorithm);
            comparisonSource = "expectedChecksum";
        } else if (options.isValidateAgainstServer()) {
            if (algorithm != Algorithm.MD5) {
                throw new IllegalArgumentException(
                    "validateChecksum against Azure only supports MD5; got " + algorithm
                        + ". Provide expectedChecksum for " + algorithm + "."
                );
            }
            if (serverContentMd5 == null || serverContentMd5.length == 0) {
                String msg = "No Content-MD5 stored on server for '" + resourceName
                    + "'. This is common for block blobs uploaded as streams.";
                if (options.isFailOnMissingServerChecksum()) {
                    runContext.metric(Counter.of("checksum.validated", 1, "result", "missing"));
                    throw new IOException(msg);
                }
                logger.warn("{} Skipping checksum validation.", msg);
                runContext.metric(Counter.of("checksum.validated", 1, "result", "skipped"));
                return;
            }
            expectedHex = HexFormat.of().formatHex(serverContentMd5);
            comparisonSource = "server Content-MD5";
        } else {
            return;
        }

        String actualHex = computeHex(file, algorithm);

        if (!actualHex.equalsIgnoreCase(expectedHex)) {
            runContext.metric(Counter.of("checksum.validated", 1, "result", "mismatch"));
            throw new IOException(
                "Checksum mismatch for '" + resourceName + "' using " + algorithm
                    + " (source: " + comparisonSource + "). Expected=" + expectedHex
                    + " actual=" + actualHex
            );
        }

        runContext.metric(Counter.of("checksum.validated", 1, "result", "match"));
        logger.debug("Checksum {} verified for '{}' (source: {})", algorithm, resourceName, comparisonSource);
    }

    private static String computeHex(File file, Algorithm algorithm) throws IOException {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance(algorithm.javaName());
        } catch (NoSuchAlgorithmException e) {
            throw new IOException("Algorithm " + algorithm + " not available", e);
        }

        try (InputStream in = new FileInputStream(file)) {
            byte[] buffer = new byte[8192];
            int read;
            while ((read = in.read(buffer)) != -1) {
                digest.update(buffer, 0, read);
            }
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private static String normalizeExpected(String expected, Algorithm algorithm) {
        String trimmed = expected.trim();
        int expectedHexLength = expectedDigestBytes(algorithm) * 2;

        if (trimmed.length() == expectedHexLength && trimmed.matches("[0-9a-fA-F]+")) {
            return trimmed;
        }
        try {
            byte[] decoded = Base64.getDecoder().decode(trimmed);
            if (decoded.length == expectedDigestBytes(algorithm)) {
                return HexFormat.of().formatHex(decoded);
            }
        } catch (IllegalArgumentException ignored) {
        }
        throw new IllegalArgumentException(
            "expectedChecksum '" + expected + "' is not a valid " + algorithm
                + " digest (expected " + expectedHexLength + " hex chars or matching base64)."
        );
    }

    private static int expectedDigestBytes(Algorithm algorithm) {
        return switch (algorithm) {
            case MD5 -> 16;
            case SHA_256 -> 32;
        };
    }
}
