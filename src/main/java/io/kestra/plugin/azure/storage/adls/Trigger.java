package io.kestra.plugin.azure.storage.adls;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.AbstractConnectionInterface;
import io.kestra.plugin.azure.AzureClientWithSasInterface;
import io.kestra.plugin.azure.storage.adls.models.AdlsFile;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.time.Duration;
import java.util.Optional;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Run a flow as soon as files are uploaded to Azure Data Lake Storage",
    description = "This trigger will poll the specified Azure Data Lake Storage bucket every `interval`. " +
    "Using the `from` and `regExp` properties, you can define which files arrival will trigger the flow. " +
    "Under the hood, we use the Azure Data Lake Storage API to list the files in a specified location and download them to the internal storage and process them with the declared `action`. " +
    "You can use the `action` property to move or delete the files from the container after processing to avoid the trigger to be fired again for the same files during the next polling interval."
)
@Plugin(
    examples = {
        @Example(
            title = "Run a flow if one or more files arrived in the specified Azure Data Lake Storage bucket location. Then, process all files in a for-loop either sequentially or concurrently, depending on the `concurrencyLimit` property.",
            full = true,
            code = """
                id: react_to_files
                namespace: company.team

                tasks:
                  - id: each
                    type: io.kestra.plugin.core.flow.ForEach
                    concurrencyLimit: 1
                    values: "{{ trigger.files | jq('.[].uri') }}"
                    tasks:
                      - id: return
                        type: io.kestra.plugin.core.debug.Return
                        format: "{{ taskrun.value }}"

                triggers:
                  - id: watch
                    type: io.kestra.plugin.azure.storage.adls.Trigger
                    interval: PT5M
                    endpoint: "https://yourblob.blob.core.windows.net"
                    connectionString: "DefaultEndpointsProtocol=...=="
                    fileSystem: myFileSystem
                    directoryPath: yourDirectory/subdirectory
                """
        )
    }
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<List.Output>, AbstractConnectionInterface, AzureClientWithSasInterface {

    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    protected String endpoint;

    protected String connectionString;

    protected String sharedKeyAccountName;

    protected String sharedKeyAccountAccessKey;

    protected String sasToken;

    private String fileSystem;

    private String directoryPath;

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();

        List task = List.builder()
            .id(this.id)
            .type(List.class.getName())
            .endpoint(this.endpoint)
            .connectionString(this.connectionString)
            .sharedKeyAccountName(this.sharedKeyAccountName)
            .sharedKeyAccountAccessKey(this.sharedKeyAccountAccessKey)
            .sasToken(this.sasToken)
            .fileSystem(this.fileSystem)
            .directoryPath(this.directoryPath)
            .build();
        List.Output run = task.run(runContext);

        if (run.getFiles().isEmpty()) {
            return Optional.empty();
        }

        java.util.List<AdlsFile> list = run
            .getFiles()
            .stream()
            .map(throwFunction(object -> {
                Read download = Read.builder()
                    .id(this.id)
                    .type(List.class.getName())
                    .endpoint(this.endpoint)
                    .connectionString(this.connectionString)
                    .sharedKeyAccountName(this.sharedKeyAccountName)
                    .sharedKeyAccountAccessKey(this.sharedKeyAccountAccessKey)
                    .sasToken(this.sasToken)
                    .fileSystem(this.fileSystem)
                    .filePath(object.getName())
                    .build();
                Read.Output downloadOutput = download.run(runContext);

                return object.withUri(downloadOutput.getFile().getUri());
            }))
            .toList();

        Execution execution = TriggerService.generateExecution(this,
            conditionContext,
            context,
            List.Output.builder().files(list).build()
        );

        return Optional.of(execution);
    }
}
