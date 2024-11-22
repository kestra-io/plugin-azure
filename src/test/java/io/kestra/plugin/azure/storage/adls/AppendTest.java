package io.kestra.plugin.azure.storage.adls;

import com.google.common.io.CharStreams;
import io.kestra.core.models.property.Property;
import io.kestra.core.utils.IdUtils;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class AppendTest extends AbstractTest {
    @Test
    void run() throws Exception {
        String prefix = IdUtils.create();

        Upload.Output upload = uploadStringFile("adls/azure/" + prefix);

        // append to file
        final String dataToAdd = "added Data";

        Append append = Append.builder()
            .id(AppendTest.class.getSimpleName())
            .type(Append.class.getName())
            .endpoint(this.adlsEndpoint)
            .connectionString(connectionString)
            .fileSystem(this.fileSystem)
            .fileName(upload.getFile().getName())
            .data(Property.of(dataToAdd))
            .build();

        append.run(runContext(append));

        // download
        Read download = Read.builder()
            .id(AppendTest.class.getSimpleName())
            .type(Read.class.getName())
            .endpoint(this.adlsEndpoint)
            .connectionString(connectionString)
            .fileSystem(this.fileSystem)
            .fileName(upload.getFile().getName())
            .build();

        Read.Output run = download.run(runContext(download));

        InputStream get = storageInterface.get(null, null, run.getFile().getUri());

        assertThat(
            CharStreams.toString(new InputStreamReader(get)),
            is(CharStreams.toString(new InputStreamReader(new FileInputStream(file("testFiles/appendTest.txt")))).concat(dataToAdd))
        );
    }
}
