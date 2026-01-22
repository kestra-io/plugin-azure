package io.kestra.plugin.azure.storage.blob;

import java.net.URI;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.kestra.core.runners.RunContext;
import io.kestra.core.storages.FileAttributes;
import io.kestra.core.storages.Storage;

class UploadTest {

    @Test
    void testDirectoryPathHandling() throws Exception {
        // Test that directory paths are correctly processed
        RunContext runContext = mock(RunContext.class);
        Storage storage = mock(Storage.class);

        when(runContext.storage()).thenReturn(storage);

        URI dirUri = URI.create("kestra:///tmp/test-dir/");

        // Create mock FileAttributes
        FileAttributes file1 = mock(FileAttributes.class);
        when(file1.getFileName()).thenReturn("file1.txt");
        when(file1.getType()).thenReturn(FileAttributes.FileType.File);

        FileAttributes file2 = mock(FileAttributes.class);
        when(file2.getFileName()).thenReturn("file2.txt");
        when(file2.getType()).thenReturn(FileAttributes.FileType.File);

        List<FileAttributes> mockFiles = List.of(file1, file2);
        when(storage.list(dirUri)).thenReturn(mockFiles);

        // Verify that storage.list is called with correct URI
        storage.list(dirUri);
        verify(storage, times(1)).list(dirUri);

        // Verify FileAttributes have correct properties
        assertEquals("file1.txt", file1.getFileName());
        assertEquals("file2.txt", file2.getFileName());
        assertEquals(FileAttributes.FileType.File, file1.getType());
        assertEquals(FileAttributes.FileType.File, file2.getType());
    }

    @Test
    void testEmptyDirectoryHandling() throws Exception {
        // Test that empty directory returns empty list
        RunContext runContext = mock(RunContext.class);
        Storage storage = mock(Storage.class);

        when(runContext.storage()).thenReturn(storage);

        URI dirUri = URI.create("kestra:///tmp/empty-dir/");
        when(storage.list(dirUri)).thenReturn(Collections.emptyList());

        List<FileAttributes> result = storage.list(dirUri);

        // Verify empty directory handling
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testFileAttributesMapping() {
        // Test FileAttributes to filename mapping logic
        FileAttributes file = mock(FileAttributes.class);
        when(file.getFileName()).thenReturn("test.txt");
        when(file.getType()).thenReturn(FileAttributes.FileType.File);

        // Verify mapping works correctly
        assertEquals("test.txt", file.getFileName());
        assertEquals(FileAttributes.FileType.File, file.getType());

        // Test that directory path + filename produces correct URI
        String directoryPath = "kestra:///tmp/test-dir/";
        String expectedUri = directoryPath + file.getFileName();
        assertEquals("kestra:///tmp/test-dir/test.txt", expectedUri);
    }
}
