package io.kestra.plugin.azure.storage.blob;

import com.azure.storage.blob.BlobContainerClient;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.azure.AzureTest;
import io.kestra.plugin.azure.storage.blob.models.CopyListDestination;
import io.kestra.plugin.azure.storage.blob.models.CopyListSource;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CopyListTest extends AzureTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void testCopyList() throws Exception {
        RunContext runContext = runContextFactory.of();
        
        String sourceContainer = IdUtils.create().toLowerCase();
        String destContainer = IdUtils.create().toLowerCase();

        // Create containers
        BlobContainerClient sourceClient = storageClient.createBlobContainer(sourceContainer);
        BlobContainerClient destClient = storageClient.createBlobContainer(destContainer);

        // Upload test files
        byte[] content = "Hello Kestra".getBytes(StandardCharsets.UTF_8);
        sourceClient.getBlobClient("inbound/file1.txt").upload(new ByteArrayInputStream(content), content.length);
        sourceClient.getBlobClient("inbound/file2.txt").upload(new ByteArrayInputStream(content), content.length);

        // Configure task
        CopyList task = CopyList.builder()
            .endpoint(this.endpoint)
            .connectionString(this.connectionString)
            .from(CopyListSource.builder()
                .container(sourceContainer)
                .prefix("inbound/")
                .build())
            .to(CopyListDestination.builder()
                .container(destContainer)
                .prefix("archive/")
                .build())
            .delete(Property.ofValue(true)) // Test MOVE operation
            .build();

        CopyList.Output output = task.run(runContext);

        // Assertions
        assertThat(output.getCount(), is(2L));
        
        // Assert files exist in destination
        assertTrue(destClient.getBlobClient("archive/file1.txt").exists());
        assertTrue(destClient.getBlobClient("archive/file2.txt").exists());

        // Assert files were deleted from source (since delete = true)
        assertTrue(!sourceClient.getBlobClient("inbound/file1.txt").exists());
    }
}