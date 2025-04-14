package io.kestra.plugin.azure.storage.adls;

import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.AbstractConnectionInterface;
import io.kestra.plugin.azure.AzureClientWithSasInterface;
import io.kestra.plugin.azure.storage.adls.models.AdlsFile;
import io.kestra.plugin.azure.storage.adls.services.DataLakeService;
import io.kestra.plugin.azure.storage.blob.Copy;
import io.kestra.plugin.azure.storage.blob.abstracts.ActionInterface;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
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
    title = "Trigger a flow on new file arrival in Azure Data Lake Storage.",
    description = "This trigger will poll the specified Azure Data Lake Storage bucket every `interval`. " +
        "Using the `from` and `regExp` properties, you can define which file's arrival will trigger the flow. " +
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

    protected Property<String> endpoint;

    protected Property<String> connectionString;

    protected Property<String> sharedKeyAccountName;

    protected Property<String> sharedKeyAccountAccessKey;

    protected Property<String> sasToken;

    private Property<String> fileSystem;

    private Property<String> directoryPath;

    @Schema(
        title = "The action to perform on the retrieved files. If using `NONE`, make sure to handle the files inside your flow to avoid infinite triggering."
    )
    @Builder.Default
    @NotNull
    private Property<Action> action = Property.of(Action.NONE);

    @Schema(
        title = "The destination container and key."
    )
    @PluginProperty(dynamic = true)
    DestinationObject moveTo;

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
                    .filePath(Property.of(object.getName()))
                    .build();
                Read.Output downloadOutput = download.run(runContext);

                return downloadOutput.getFile();
            }))
            .toList();

        DataLakeServiceClient client = DataLakeService.client(
            runContext.render(endpoint).as(String.class).orElse(null),
            runContext.render(connectionString).as(String.class).orElse(null),
            runContext.render(sharedKeyAccountName).as(String.class).orElse(null),
            runContext.render(sharedKeyAccountAccessKey).as(String.class).orElse(null),
            runContext.render(sasToken).as(String.class).orElse(null),
            runContext);

        //Create the target directory in the target fileSystem for MOVE action
        if (Action.MOVE.equals(runContext.render(this.action).as(Action.class).orElseThrow())) {
            final String toDirPath = runContext.render(this.moveTo.getDirectoryPath()).as(String.class).orElseThrow();
            client.getFileSystemClient(runContext.render(this.moveTo.getFileSystem()).as(String.class).orElseThrow())
                .createDirectoryIfNotExists(toDirPath);

        }

        for (AdlsFile file : list) {
            switch (runContext.render(this.action).as(Action.class).orElseThrow()) {
                case DELETE -> {
                    Delete delete = Delete.builder()
                        .id(this.id)
                        .type(Delete.class.getName())
                        .endpoint(this.endpoint)
                        .connectionString(this.connectionString)
                        .sharedKeyAccountName(this.sharedKeyAccountName)
                        .sharedKeyAccountAccessKey(this.sharedKeyAccountAccessKey)
                        .sasToken(this.sasToken)
                        .fileSystem(this.fileSystem)
                        .filePath(Property.of(file.getName()))
                        .build();
                    delete.run(runContext);
                }
                case MOVE -> {
                    DataLakeFileClient fileClient = client.getFileSystemClient(runContext.render(this.fileSystem).as(String.class).orElseThrow())
                        .getFileClient(file.getName());

                    fileClient.rename(
                        runContext.render(this.moveTo.getFileSystem()).as(String.class).orElseThrow(),
                        runContext.render(this.moveTo.getDirectoryPath() + "/" + fileClient.getFileName())
                    );
                }
                default -> runContext.logger().debug("NONE action is selected for this trigger.");
            }
        }


        Execution execution = TriggerService.generateExecution(this,
            conditionContext,
            context,
            List.Output.builder().files(list).build()
        );

        return Optional.of(execution);
    }

    public enum Action {
        MOVE,
        DELETE,
        NONE
    }

    @SuperBuilder(toBuilder = true)
    @Getter
    @NoArgsConstructor
    public static class DestinationObject {
        @Schema(
            title = "The destination file system."
        )
        @NotNull
        Property<String> fileSystem;

        @Schema(
            title = "The full destination directory path on the file system."
        )
        @NotNull
        Property<String> directoryPath;
    }
}
