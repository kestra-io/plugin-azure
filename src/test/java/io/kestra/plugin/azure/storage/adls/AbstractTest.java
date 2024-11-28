package io.kestra.plugin.azure.storage.adls;

import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.azure.BaseTest;
import io.kestra.plugin.azure.storage.adls.services.DataLakeService;
import org.junit.jupiter.api.AfterEach;

import java.net.URI;
import java.util.ArrayList;

public class AbstractTest extends BaseTest {
    public java.util.List<String> directoryToClean = new ArrayList<>();

    protected Upload.Output upload(String dir) throws Exception {
        URI source = upload();

        String out = IdUtils.create();

        Upload upload = Upload.builder()
            .id(AllTest.class.getSimpleName())
            .type(io.kestra.plugin.azure.storage.blob.Upload.class.getName())
            .endpoint(this.adlsEndpoint)
            .connectionString(connectionString)
            .fileSystem(this.fileSystem)
            .from(source.toString())
            .filePath(dir + "/" + out + ".yml")
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
            .endpoint(this.adlsEndpoint)
            .connectionString(connectionString)
            .fileSystem(this.fileSystem)
            .from(source.toString())
            .filePath(dir + "/" + out + ".txt")
            .build();

        directoryToClean.add(dir);

        return upload.run(runContext(upload));
    }

    protected List.ListBuilder<?, ?> list() {
        return List.builder()
            .id(AbstractTest.class.getSimpleName())
            .type(List.class.getName())
            .endpoint(this.adlsEndpoint)
            .connectionString(connectionString)
            .fileSystem(this.fileSystem);
    }

    protected DeleteFiles.DeleteFilesBuilder<?, ?> deleteDir(String dir) {
        return DeleteFiles.builder()
            .id(AbstractTest.class.getSimpleName())
            .type(DeleteFiles.class.getName())
            .endpoint(this.adlsEndpoint)
            .connectionString(connectionString)
            .fileSystem(this.fileSystem)
            .directoryPath(dir);
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
