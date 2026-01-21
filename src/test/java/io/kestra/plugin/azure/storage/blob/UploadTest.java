package io.kestra.plugin.azure.storage.blob;

import io.kestra.plugin.azure.storage.blob.models.Blob;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class UploadTest {

    RunContext runContext = mock(RunContext.class);

    @Test
    void testDirectoryUploadMultipleFiles() throws Exception {
        // Mock storage to return multiple files for a directory
        URI dirUri = new URI("file:///tmp/test-dir/");
        List<URI> mockFiles = List.of(
            new URI("file:///tmp/test-dir/file1.txt"),
            new URI("file:///tmp/test-dir/file2.txt")
        );

        when(runContext.storage().list(dirUri)).thenReturn(mockFiles);

        Upload task = Upload.builder()
            .from(Property.of(dirUri.toString()))
            .build();

        Upload.Output output = task.run(runContext);

        // Check that blobs list has all uploaded files
        assertEquals(2, output.getBlobs().size());

        // For directory upload, blob (single) should be null
        assertNull(output.getBlob());

        // Optionally, verify metadata/tags applied to all
        for (Blob blob : output.getBlobs()) {
            assertNotNull(blob);
        }
    }

    @Test
    void testEmptyDirectoryUpload() throws Exception {
        URI dirUri = new URI("file:///tmp/empty-dir/");
        when(runContext.storage().list(dirUri)).thenReturn(Collections.emptyList());

        Upload task = Upload.builder()
            .from(Property.of(dirUri.toString()))
            .build();

        // Should throw exception for empty directory
        assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
    }
}
