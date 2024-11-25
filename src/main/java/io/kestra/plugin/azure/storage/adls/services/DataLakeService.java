package io.kestra.plugin.azure.storage.adls.services;

import com.azure.core.credential.AzureNamedKeyCredential;
import com.azure.core.http.rest.PagedIterable;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.azure.storage.file.datalake.models.ListPathsOptions;
import com.azure.storage.file.datalake.models.PathItem;
import com.azure.storage.file.datalake.models.PathProperties;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.FileUtils;
import io.kestra.plugin.azure.storage.adls.models.AdlsFile;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class DataLakeService {
    public static URI read(RunContext runContext, DataLakeFileClient client) throws IOException {
        File tempFile = runContext.workingDir().createTempFile(FileUtils.getExtension(client.getFileName())).toFile();
        PathProperties pathProperties = client.readToFile(tempFile.getAbsolutePath(), true);

        runContext.metric(Counter.of("file.size", pathProperties.getFileSize()));

        return runContext.storage().putFile(tempFile);
    }

    public static List<AdlsFile> list(RunContext runContext, DataLakeFileSystemClient fileSystemClient, String directoryPath) throws IllegalVariableEvaluationException {
        ListPathsOptions options = new ListPathsOptions();
        options.setPath(runContext.render(directoryPath));

        PagedIterable<PathItem> pagedIterable = fileSystemClient.listPaths(options, Duration.ofSeconds(30L));

        java.util.Iterator<PathItem> iterator = pagedIterable.iterator();

        List<AdlsFile> fileList = new ArrayList<>();

        while (iterator.hasNext()) {
            PathItem item = iterator.next();
            fileList.add(AdlsFile.of(fileSystemClient.getFileSystemName(), item));
        }
        return fileList;
    }

    public static DataLakeServiceClient client(
        String endpoint,
        String connectionString,
        String sharedKeyAccountName,
        String sharedKeyAccountAccessKey,
        String sasToken,
        RunContext runContext
    ) throws IllegalVariableEvaluationException {
        DataLakeServiceClientBuilder builder = new DataLakeServiceClientBuilder();

        if (endpoint != null) {
            builder.endpoint(runContext.render(endpoint));
        }

        if (connectionString != null) {
            builder.connectionString(runContext.render(connectionString));
        } else if (sharedKeyAccountName != null && sharedKeyAccountAccessKey != null) {
            builder.credential(new AzureNamedKeyCredential(
                runContext.render(sharedKeyAccountName),
                runContext.render(sharedKeyAccountAccessKey)
            ));
        } else if (sasToken != null ) {
            builder.sasToken(runContext.render(sasToken));
        } else {
            builder.credential(new DefaultAzureCredentialBuilder().build());
        }


        return builder.buildClient();
    }
}
