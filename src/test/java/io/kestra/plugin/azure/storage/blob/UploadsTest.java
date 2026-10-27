package io.kestra.plugin.azure.storage.blob;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.azure.storage.blob.models.Blob;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@MicronautTest
class UploadsTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    // Adjust these to match your local Azurite / Azure test environment bindings
    private final String endpoint = "http://127.0.0.1:10000/devstoreaccount1";
    private final String connectionString = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;";
    private final String container = "test-container-" + IdUtils.create().toLowerCase();

    @Test
    void testUploadsFromList() throws Exception {
        RunContext runContext = runContextFactory.of();

        // 1. Create mock files in internal storage
        URI file1 = storageInterface.put(
            null, null, new URI("/" + IdUtils.create() + "/file1.txt"), 
            new ByteArrayInputStream("File 1 Content".getBytes())
        );
        URI file2 = storageInterface.put(
            null, null, new URI("/" + IdUtils.create() + "/file2.txt"), 
            new ByteArrayInputStream("File 2 Content".getBytes())
        );

        // 2. Configure task
        Uploads task = Uploads.builder()
            .id(IdUtils.create())
            .type(Uploads.class.getName())
            .endpoint(endpoint)
            .connectionString(connectionString)
            .container(container)
            .prefix("list-upload/")
            .from(List.of(file1.toString(), file2.toString()))
            .build();

        // 3. Run and assert
        Uploads.Output runOutput = task.run(runContext);
        
        assertThat(runOutput.getCount(), is(2));
        assertThat(runOutput.getBlobs().size(), is(2));
        
        List<String> blobNames = runOutput.getBlobs().stream().map(Blob::getName).toList();
        assertThat(blobNames, containsInAnyOrder("list-upload/file1.txt", "list-upload/file2.txt"));
    }

    @Test
    void testUploadsFromMap() throws Exception {
        RunContext runContext = runContextFactory.of();

        URI file1 = storageInterface.put(
            null, null, new URI("/" + IdUtils.create() + "/internal_data1.csv"), 
            new ByteArrayInputStream("a,b,c".getBytes())
        );
        URI file2 = storageInterface.put(
            null, null, new URI("/" + IdUtils.create() + "/internal_data2.csv"), 
            new ByteArrayInputStream("d,e,f".getBytes())
        );

        // Map relative sub-paths to the files
        Map<String, String> mapFrom = Map.of(
            "raw/data/part1.csv", file1.toString(),
            "raw/data/part2.csv", file2.toString()
        );

        Uploads task = Uploads.builder()
            .id(IdUtils.create())
            .type(Uploads.class.getName())
            .endpoint(endpoint)
            .connectionString(connectionString)
            .container(container)
            .from(mapFrom)
            .metadata(Property.of(Map.of("category", "test")))
            .build();

        Uploads.Output runOutput = task.run(runContext);
        
        assertThat(runOutput.getCount(), is(2));
        assertThat(runOutput.getOutputFiles().keySet(), containsInAnyOrder("raw/data/part1.csv", "raw/data/part2.csv"));
        
        // Assert metadata mapping applied correctly
        assertThat(runOutput.getBlobs().get(0).getMetadata().get("category"), is("test"));
    }
}