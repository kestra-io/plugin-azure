package io.kestra.plugin.azure.runner;

import io.kestra.core.models.script.*;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.azure.storage.blob.models.BlobStorageForBatch;
import io.kestra.plugin.scripts.exec.scripts.runners.CommandsWrapper;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
@Disabled("Too costly to run on CI")
public class AzureBatchScriptRunnerTest extends AbstractScriptRunnerTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.variables.globals.azure.batch.accessKey}")
    private String accessKey;

    @Value("${kestra.variables.globals.azure.batch.account}")
    private String account;

    @Value("${kestra.variables.globals.azure.batch.endpoint}")
    private String endpoint;

    @Value("${kestra.variables.globals.azure.batch.poolId}")
    private String poolId;

    @Value("${kestra.variables.globals.azure.blobs.connection-string}")
    protected String connectionString;

    @Value("${kestra.variables.globals.azure.batch.blobs.endpoint}")
    private String blobEndpoint;

    @Value("${kestra.variables.globals.azure.batch.blobs.containerName}")
    private String blobContainerName;

    @Override
    @Test
    protected void inputAndOutputFiles() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of("internalStorageFile", "kestra://some/internalStorage.txt"));

        // Generate input file
        Path workingDirectory = runContext.tempDir();
        File file = workingDirectory.resolve("hello.txt").toFile();
        FileUtils.writeStringToFile(file, "Hello World", "UTF-8");

        // Generate internal storage file
        FileUtils.writeStringToFile(Path.of("/tmp/unittest/internalStorage.txt").toFile(), "Hello from internal storage", StandardCharsets.UTF_8);

        DefaultLogConsumer defaultLogConsumer = new DefaultLogConsumer(runContext);
        // This is purely to showcase that no logs is sent as STDERR for now as CloudWatch doesn't seem to send such information.
        Map<String, Boolean> logsWithIsStdErr = new HashMap<>();
        CommandsWrapper commandsWrapper = new CommandsWrapper(runContext)
            .withCommands(ScriptService.scriptCommands(List.of("/bin/sh", "-c"), null, List.of(
                "cat {{internalStorageFile}}",
                "cat hello.txt",
                "cat hello.txt > output.txt",
                "echo '::{\"outputs\":{\"logOutput\":\"Hello World\"}}::'"
            )))
            .withContainerImage("ubuntu")
            .withLogConsumer(new AbstractLogConsumer() {
                @Override
                public void accept(String log, Boolean isStdErr) {
                    logsWithIsStdErr.put(log, isStdErr);
                    defaultLogConsumer.accept(log, isStdErr);
                }
            });
        RunnerResult run = scriptRunner().run(runContext, commandsWrapper, List.of("hello.txt"), List.of("output.txt"));

        // Exit code for successful job
        assertThat(run.getExitCode(), is(0));

        // Verify logs, we can't assert exact log entries as logs are sometimes grouped together by AWS CloudWatch
        Set<Map.Entry<String, Boolean>> logEntries = logsWithIsStdErr.entrySet();
        assertThat(logEntries.stream().filter(e -> e.getKey().contains("Hello from internal storage")).findFirst().orElseThrow().getValue(), is(false));
        assertThat(logEntries.stream().filter(e -> e.getKey().contains("Hello World")).findFirst().orElseThrow().getValue(), is(false));

        // Verify outputFiles
        File outputFile = runContext.resolve(Path.of("output.txt")).toFile();
        assertThat(outputFile.exists(), is(true));
        assertThat(FileUtils.readFileToString(outputFile, StandardCharsets.UTF_8), is("Hello World"));

        assertThat(defaultLogConsumer.getOutputs().get("logOutput"), is("Hello World"));
    }

    @Override
    protected ScriptRunner scriptRunner() {
        return AzureBatchScriptRunner.builder()
            .accessKey(accessKey)
            .account(account)
            .endpoint(endpoint)
            .blobStorage(
                BlobStorageForBatch.builder()
                    .endpoint(blobEndpoint)
                    .containerName(blobContainerName)
                    .connectionString(connectionString)
                    .build()
            )
            .poolId(poolId)
            .waitUntilCompletion(Duration.ofMinutes(30))
            .build();
    }
}
