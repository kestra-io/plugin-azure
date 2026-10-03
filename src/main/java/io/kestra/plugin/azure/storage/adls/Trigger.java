package io.kestra.plugin.azure.storage.adls;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.LoggerFactory;

import com.azure.storage.file.datalake.models.ListPathsOptions;
import com.fasterxml.jackson.annotation.JsonUnwrapped;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.utils.FileUtils;
import io.kestra.plugin.azure.shared.AbstractConnectionInterface;
import io.kestra.plugin.azure.shared.AzureClientWithSasInterface;
import io.kestra.plugin.azure.storage.adls.models.AdlsFile;
import io.kestra.plugin.azure.storage.adls.services.DataLakeService;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.Disposable;

import static io.kestra.core.models.triggers.StatefulTriggerService.*;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger a flow on new file arrival in Azure Data Lake Storage",
    description = "This trigger will poll the specified Azure Data Lake Storage file system every `interval`. " +
        "Using the `from` and `regExp` properties, you can define which files' arrival will trigger the flow. " +
        "Under the hood, we use the Azure Data Lake Storage API to list the files in a specified location and download them to the internal storage and process them with the declared `action`. "
        +
        "You can use the `action` property to move or delete the files from the container after processing to avoid the trigger to be fired again for the same files during the next polling interval."
)
@Plugin(
    examples = {
        @Example(
            title = "Run a flow if one or more files arrived in the specified Azure Data Lake Storage file system location. Then, process all files in a for-loop either sequentially or concurrently, depending on the `concurrencyLimit` property.",
            full = true,
            code = """
                id: react_to_files
                namespace: company.team

                tasks:
                  - id: each
                    type: io.kestra.plugin.core.flow.Loop
                    concurrencyLimit: 1
                    values: "{{ trigger.files | jq('.[].uri') }}"
                    tasks:
                      - id: return
                        type: io.kestra.plugin.core.debug.Return
                        format: "{{ item.value }}"

                triggers:
                  - id: watch
                    type: io.kestra.plugin.azure.storage.adls.Trigger
                    interval: PT5M
                    endpoint: "https://yourblob.blob.core.windows.net"
                    connectionString: "{{ secret('AZURE_CONNECTION_STRING') }}"
                    fileSystem: myFileSystem
                    directoryPath: yourDirectory/subdirectory
                """
        )
    }
)
public class Trigger extends AbstractTrigger
    implements PollingTriggerInterface, TriggerOutput<Trigger.Output>, AbstractConnectionInterface, AzureClientWithSasInterface, StatefulTriggerInterface {

    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    protected Property<String> endpoint;

    @PluginProperty(group = "connection", secret = true)
    protected Property<String> connectionString;

    protected Property<String> sharedKeyAccountName;

    @PluginProperty(group = "connection", secret = true)
    protected Property<String> sharedKeyAccountAccessKey;

    @PluginProperty(group = "connection", secret = true)
    protected Property<String> sasToken;

    @Schema(title = "The ADLS file system (container) to monitor")
    private Property<String> fileSystem;

    @Schema(title = "The directory path to monitor")
    private Property<String> directoryPath;

    @Schema(
        title = "The action to perform on the retrieved files. If using `NONE`, make sure to handle the files inside your flow to avoid infinite triggering"
    )
    @Builder.Default
    @NotNull
    @PluginProperty(group = "main")
    private Property<Action> action = Property.ofValue(Action.NONE);

    @Schema(
        title = "The destination container and key"
    )
    @PluginProperty(dynamic = true, group = "destination")
    DestinationObject moveTo;

    @Schema(
        title = "The maximum number of files to retrieve at once",
        description = "Limits the number of files retrieved per polling interval. If not specified, all matching files will be retrieved."
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<Integer> maxFiles = Property.ofValue(25);

    @Builder.Default
    private final Property<On> on = Property.ofValue(On.CREATE_OR_UPDATE);

    private Property<String> stateKey;

    private Property<Duration> stateTtl;

    @Builder.Default
    @ToString.Exclude
    @Getter(AccessLevel.NONE)
    private transient AtomicReference<Disposable> disposable = new AtomicReference<>();

    @Builder.Default
    @ToString.Exclude
    @Getter(AccessLevel.NONE)
    private transient AtomicBoolean killed = new AtomicBoolean(false);

    @Builder.Default
    @ToString.Exclude
    @Getter(AccessLevel.NONE)
    private transient AtomicReference<CountDownLatch> latchRef = new AtomicReference<>();

    @Override
    public void kill() {
        killed.compareAndSet(false, true);
        cancelInFlight();
    }

    private void cancelInFlight() {
        Disposable current = disposable.getAndSet(null);
        if (current != null) {
            try {
                current.dispose();
            } catch (Exception e) {
                LoggerFactory.getLogger(this.getClass()).warn("Failed to dispose subscription", e);
            }
        }
        CountDownLatch latch = latchRef.get();
        if (latch != null) {
            latch.countDown();
        }
    }

    private void await(CountDownLatch latch, Disposable currentDisposable) throws InterruptedException {
        this.latchRef.set(latch);
        this.disposable.set(currentDisposable);
        if (killed.get()) {
            this.cancelInFlight();
        }
        try {
            latch.await();
        } finally {
            this.cancelInFlight();
        }
    }

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        var runContext = conditionContext.getRunContext();

        var rOn = runContext.render(on).as(On.class).orElse(On.CREATE_OR_UPDATE);
        var rStateKey = runContext.render(stateKey).as(String.class).orElse(StatefulTriggerService.defaultKey(context.getNamespace(), context.getFlowId(), id));
        var rStateTtl = runContext.render(stateTtl).as(Duration.class);

        var dataLakeServiceAsyncClient = DataLakeService.asyncClient(
            runContext.render(this.endpoint).as(String.class).orElse(null),
            runContext.render(this.connectionString).as(String.class).orElse(null),
            runContext.render(this.sharedKeyAccountName).as(String.class).orElse(null),
            runContext.render(this.sharedKeyAccountAccessKey).as(String.class).orElse(null),
            runContext.render(this.sasToken).as(String.class).orElse(null),
            runContext
        );
        var fileSystemAsyncClient = dataLakeServiceAsyncClient.getFileSystemAsyncClient(runContext.render(fileSystem).as(String.class).orElseThrow());

        var rDirectoryPath = runContext.render(directoryPath).as(String.class).orElseThrow();
        var options = new ListPathsOptions();
        options.setPath(rDirectoryPath);
        var rMaxFiles = runContext.render(this.maxFiles).as(Integer.class).orElse(25);

        var listLatch = new CountDownLatch(1);
        var error = new AtomicReference<Throwable>();
        var fileList = new ArrayList<AdlsFile>();

        var syncClient = DataLakeService.client(
            runContext.render(this.endpoint).as(String.class).orElse(null),
            runContext.render(this.connectionString).as(String.class).orElse(null),
            runContext.render(this.sharedKeyAccountName).as(String.class).orElse(null),
            runContext.render(this.sharedKeyAccountAccessKey).as(String.class).orElse(null),
            runContext.render(this.sasToken).as(String.class).orElse(null),
            runContext
        );
        var syncFsClient = syncClient.getFileSystemClient(runContext.render(fileSystem).as(String.class).orElseThrow());

        var listDisposable = fileSystemAsyncClient.listPaths(options)
            .map(item -> AdlsFile.of(syncFsClient.getFileClient(item.getName())))
            .take(rMaxFiles)
            .subscribe(
                fileList::add,
                err ->
                {
                    error.set(err);
                    listLatch.countDown();
                },
                listLatch::countDown
            );

        this.await(listLatch, listDisposable);

        if (killed.get())
            return Optional.empty();
        if (error.get() != null) {
            if (error.get() instanceof Exception e)
                throw e;
            throw new Exception(error.get());
        }

        if (fileList.isEmpty()) {
            return Optional.empty();
        }

        var state = readState(runContext, rStateKey, rStateTtl);
        var toFire = new ArrayList<TriggeredFile>();

        for (var file : fileList) {
            if (killed.get())
                return Optional.empty();

            var uri = String.format("adls://%s/%s", runContext.render(fileSystem).as(String.class).orElse(""), file.getName());
            var modifiedAt = Optional.ofNullable(file.getLastModifed()).orElse(Instant.now());
            var version = Optional.ofNullable(file.getETag()).orElse(String.valueOf(modifiedAt.toEpochMilli()));

            var candidate = StatefulTriggerService.Entry.candidate(uri, version, modifiedAt);
            var stateChange = computeAndUpdateState(state, candidate, rOn);

            if (stateChange.fire()) {
                var changeType = stateChange.isNew() ? ChangeType.CREATE : ChangeType.UPDATE;

                var tempFile = runContext.workingDir().createTempFile(FileUtils.getExtension(file.getFileName())).toFile();
                var dlLatch = new CountDownLatch(1);
                error.set(null);

                var fileAsyncClient = fileSystemAsyncClient.getFileAsyncClient(file.getName());
                var dlDisposable = fileAsyncClient.readToFile(tempFile.getAbsolutePath(), true)
                    .subscribe(
                        props ->
                        {
                            dlLatch.countDown();
                        },
                        err ->
                        {
                            error.set(err);
                            dlLatch.countDown();
                        },
                        dlLatch::countDown
                    );

                this.await(dlLatch, dlDisposable);

                if (killed.get())
                    return Optional.empty();
                if (error.get() != null) {
                    if (error.get() instanceof Exception e)
                        throw e;
                    throw new Exception(error.get());
                }

                var readFileUri = runContext.storage().putFile(tempFile);
                var downloadedFile = file.withUri(readFileUri);

                toFire.add(
                    TriggeredFile.builder()
                        .file(downloadedFile)
                        .changeType(changeType)
                        .build()
                );
            }
        }

        if (toFire.isEmpty()) {
            return Optional.empty();
        }

        if (killed.get()) {
            return Optional.empty();
        }

        // --- ATOMIC COMMIT PHASE ---
        // Create the target directory in the target fileSystem for MOVE action
        if (Action.MOVE.equals(runContext.render(this.action).as(Action.class).orElseThrow())) {
            final String toDirPath = runContext.render(this.moveTo.getDirectoryPath()).as(String.class).orElseThrow();
            syncClient.getFileSystemClient(runContext.render(this.moveTo.getFileSystem()).as(String.class).orElseThrow())
                .createDirectoryIfNotExists(toDirPath);
        }

        for (var file : toFire) {
            var adlsFile = file.getFile();

            switch (runContext.render(this.action).as(Action.class).orElseThrow()) {
                case DELETE -> {
                    var delete = Delete.builder()
                        .id(this.id)
                        .type(Delete.class.getName())
                        .endpoint(this.endpoint)
                        .connectionString(this.connectionString)
                        .sharedKeyAccountName(this.sharedKeyAccountName)
                        .sharedKeyAccountAccessKey(this.sharedKeyAccountAccessKey)
                        .sasToken(this.sasToken)
                        .fileSystem(this.fileSystem)
                        .filePath(Property.ofValue(adlsFile.getName()))
                        .build();
                    delete.run(runContext);
                }
                case MOVE -> {
                    var fileClient = syncClient.getFileSystemClient(runContext.render(this.fileSystem).as(String.class).orElseThrow())
                        .getFileClient(adlsFile.getName());

                    fileClient.rename(
                        runContext.render(this.moveTo.getFileSystem()).as(String.class).orElseThrow(),
                        runContext.render(this.moveTo.getDirectoryPath() + "/" + fileClient.getFileName())
                    );
                }
                default -> runContext.logger().debug("NONE action is selected for this trigger.");
            }
        }

        writeState(runContext, rStateKey, state, rStateTtl);

        var output = Output.builder().files(toFire).build();
        var execution = TriggerService.generateExecution(this, conditionContext, context, output);

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
            title = "The destination file system"
        )
        @NotNull
        Property<String> fileSystem;

        @Schema(
            title = "The full destination directory path on the file system"
        )
        @NotNull
        Property<String> directoryPath;
    }

    @Getter
    @AllArgsConstructor
    @Builder
    public static class TriggeredFile {
        @JsonUnwrapped
        private final AdlsFile file;
        private final ChangeType changeType;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "List of files that triggered the flow, each with its change type")
        private final java.util.List<TriggeredFile> files;
    }

    public enum ChangeType {
        CREATE,
        UPDATE
    }

}
