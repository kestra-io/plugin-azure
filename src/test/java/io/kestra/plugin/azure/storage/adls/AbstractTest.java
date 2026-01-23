package io.kestra.plugin.azure.storage.adls;

import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import io.kestra.core.models.property.Property;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.azure.BaseTest;
import io.kestra.plugin.azure.storage.adls.services.DataLakeService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.net.URI;
import java.util.ArrayList;

@EnabledIfEnvironmentVariable(named = "AZURE_CONNECTION_STRING", matches = ".+")
public class AbstractTest extends BaseTest {
    public java.util.List<String> directoryToClean = new ArrayList<>();

    protected Upload.Output upload(String dir) throws Exception {
        URI source = upload();

        String out = IdUtils.create();

        Upload upload = Upload.builder()
            .id(AllTest.class.getSimpleName())
            .type(io.kestra.plugin.azure.storage.blob.Upload.class.getName())
            .endpoint(Property.ofValue(this.adlsEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .fileSystem(Property.ofValue(this.fileSystem))
            .from(Property.ofValue(source.toString()))
            .filePath(Property.ofValue(dir + "/" + out + ".yml"))
            .build();

        directoryToClean.add(dir);

        return upload.run(runContext(upload));
    }

    protected Upload.Output uploadStringFile(String dir) throws Exception {
        URI source = uploadStringFile();

        String out = IdUtils.create();

        Upload upload = Upload.builder()
            .id(AllTest.class.getSimpleName())
            .type(io.kestra.plugin.azure.storage.blob.Upload.class.getName())
            .endpoint(Property.ofValue(this.adlsEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .fileSystem(Property.ofValue(this.fileSystem))
            .from(Property.ofValue(source.toString()))
            .filePath(Property.ofValue(dir + "/" + out + ".txt"))
            .build();

        directoryToClean.add(dir);

        return upload.run(runContext(upload));
    }

    protected List.ListBuilder<?, ?> list() {
        return List.builder()
            .id(AbstractTest.class.getSimpleName())
            .type(List.class.getName())
            .endpoint(Property.ofValue(this.adlsEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .fileSystem(Property.ofValue(this.fileSystem));
    }

    protected DeleteFiles.DeleteFilesBuilder<?, ?> deleteDir(String dir) {
        return DeleteFiles.builder()
            .id(AbstractTest.class.getSimpleName())
            .type(DeleteFiles.class.getName())
            .endpoint(Property.ofValue(this.adlsEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .fileSystem(Property.ofValue(this.fileSystem))
            .directoryPath(Property.ofValue(dir));
    }

    protected void update(String filePath) throws Exception {
        URI source = uploadStringFile();

        Upload upload = Upload.builder()
            .id(AbstractTest.class.getSimpleName())
            .type(Upload.class.getName())
            .endpoint(Property.ofValue(this.adlsEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .fileSystem(Property.ofValue(this.fileSystem))
            .from(Property.ofValue(source.toString()))
            .filePath(Property.ofValue(filePath))
            .build();

        upload.run(runContext(upload));
    }

    @AfterEach
    void cleanup() throws Exception {
        DataLakeServiceClient client = DataLakeService.client(adlsEndpoint, connectionString, null, null, null, runContextFactory.of());

        for (String dirName : directoryToClean) {
            DataLakeDirectoryClient directoryClient = client.getFileSystemClient(fileSystem).getDirectoryClient(dirName);
            if(directoryClient.exists()) {
                directoryClient.deleteRecursively();
            }
        }
    }
}
