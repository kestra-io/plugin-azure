package io.kestra.plugin.azure.storage.adls.models;

import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.models.PathProperties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.Base64;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@EnabledIfEnvironmentVariable(named = "AZURE_CONNECTION_STRING", matches = ".+")
class AdlsFileTest {

    @Test
    void shouldEncodeMd5ToBase64Safely() {
        // Use the exact MD5 byte[] corresponding to the base64 "n04CAIckGYorAyKVufEhMg=="
        byte[] md5WithNullByte = Base64.getDecoder().decode("n04CAIckGYorAyKVufEhMg==");

        // Old behavior (wrong): convert MD5 bytes as a UTF-8 string (as it was before the fix)
        String unsafeUtf8 = new String(md5WithNullByte, StandardCharsets.UTF_8);

        // Ensure the unsafe string contains a NULL character
        assertThat("Unsafe UTF-8 encoding should contain \\u0000", unsafeUtf8, containsString("\u0000"));

        // New behavior: Base64 encoding
        String safeBase64 = Base64.getEncoder().encodeToString(md5WithNullByte);

        // Ensure the base64 string is exactly as expected
        assertThat("Base64 string must match expected value", safeBase64, is("n04CAIckGYorAyKVufEhMg=="));

        // Base64 must not contain null characters
        assertThat("Base64 string must not contain \\u0000", safeBase64, not(containsString("\u0000")));

        // Now verify that AdlsFile.of() uses the correct (safe) encoding

        // Mock the DataLakeFileClient and its properties
        DataLakeFileClient mockClient = mock(DataLakeFileClient.class);
        PathProperties mockProps = mock(PathProperties.class);

        when(mockClient.getFileSystemName()).thenReturn("filesystem");
        when(mockClient.getFilePath()).thenReturn("dir/path/file.csv");
        when(mockClient.getFileName()).thenReturn("file.csv");
        when(mockClient.getProperties()).thenReturn(mockProps);
        when(mockProps.getFileSize()).thenReturn(123L);
        when(mockProps.getContentMd5()).thenReturn(md5WithNullByte);
        when(mockProps.getCreationTime()).thenReturn(OffsetDateTime.now());
        when(mockProps.getLastModified()).thenReturn(OffsetDateTime.now());

        // Build the AdlsFile using the corrected .of() method
        AdlsFile file = AdlsFile.of(mockClient);

        // Check that the contentMd5 returned is exactly the safe Base64 string
        assertThat("AdlsFile.of() should return the correct Base64 string",
            file.getContentMd5(), is("n04CAIckGYorAyKVufEhMg=="));

        // Double-check safety again
        assertThat(file.getContentMd5(), not(containsString("\u0000")));
    }
}
