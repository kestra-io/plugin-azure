package io.kestra.plugin.azure.storage.blob;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.fasterxml.jackson.annotation.JsonUnwrapped;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.plugin.azure.shared.AbstractConnectionInterface;
import io.kestra.plugin.azure.shared.AzureClientWithSasInterface;
import io.kestra.plugin.azure.shared.storage.blob.abstracts.AbstractBlobStorageContainerInterface;
import io.kestra.plugin.azure.shared.storage.blob.abstracts.ListInterface;
import io.kestra.plugin.azure.shared.storage.blob.models.Blob;
import io.kestra.plugin.azure.storage.blob.abstracts.ActionInterface;
import io.kestra.plugin.azure.storage.blob.services.BlobService;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import static io.kestra.core.models.triggers.StatefulTriggerService.*;

@SuperBuilder
@NoArgsConstructor
@Getter
@ToString
@EqualsAndHashCode

@Schema(
    title = "Trigger a flow on a new file arrival in an Azure Blob Storage container",
    description = "This trigger will poll the specified Azure Blob Storage container every `interval`. "
        + "Using the `prefix` and `regexp` properties, you can define which files' arrival will trigger the flow. "
        + "Under the hood, we use the Azure Blob Storage API to list the files in a specified location and "
        + "download them to the internal storage and process them with the declared `action`. "
        + "You can use the `action` property to move or delete the files from the container after processing "
        + "to avoid the trigger being fired again for the same files during the next polling interval."
)

@Plugin(
    examples = {
        @Example(
            title = "Run a flow if one or more files arrived in the specified Azure Blob Storage container location. "
                + "Then, process all files in a for-loop either sequentially or concurrently, depending on the "
                + "`concurrencyLimit` property.",
            full = true,
            code = """
                id: react_to_files
                namespace: company.team

                tasks:
                  - id: each
                    type: io.kestra.plugin.core.flow.Loop
                    concurrencyLimit: 1
                    values: "{{ trigger.blobs | jq('.[].uri') }}"
                    tasks:
                      - id: return
                        type: io.kestra.plugin.core.debug.Return
                        format: "{{ item.value }}"

                triggers:
                  - id: watch
                    type: io.kestra.plugin.azure.storage.blob.Trigger
                    interval: PT5M
                    endpoint: "https://yourblob.blob.core.windows.net"
                    connectionString: "{{ secret('AZURE_CONNECTION_STRING') }}"
                    container: myBlobContainer
                    prefix: yourDirectory/subdirectory
                    action: MOVE
                    moveTo:
                      container: mydata
                      name: archive
                """
        ),
        @Example(
            title = "Run a flow whenever one or more files arrived in the specified Azure Blob Storage container "
                + "location. Then, process files and delete processed files to avoid re-triggering the flow for "
                + "the same Blob objects during the next polling interval.",
            full = true,
            code = """
                id: process_and_delete_files
                namespace: company.team

                tasks:
                  - id: each
                    type: io.kestra.plugin.core.flow.Loop
                    values: "{{ trigger.blobs | jq('.[].name') }}"
                    tasks:
                      - id: return
                        type: io.kestra.plugin.core.debug.Return
                        format: "{{ item.value }}"

                      - id: delete
                        type: io.kestra.plugin.azure.storage.blob.Delete
                        endpoint: "https://yourblob.blob.core.windows.net"
                        connectionString: "{{ secret('AZURE_CONNECTION_STRING') }}"
                        container: myBlobContainer
                        name: "{{ item.value }}"

                triggers:
                  - id: watch
                    type: io.kestra.plugin.azure.storage.blob.Trigger
                    endpoint: "https://yourblob.blob.core.windows.net"
                    connectionString: "{{ secret('AZURE_CONNECTION_STRING') }}"
                    container: myBlobContainer
                    prefix: yourDirectory/subdirectory
                    action: NONE
                    moveTo:
                      container: myBlobContainer
                      name: archive
                """
        )
    }
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Trigger.Output>, AbstractConnectionInterface, ListInterface, ActionInterface,
    io.kestra.core.models.WorkerJobLifecycle,
    AbstractBlobStorageContainerInterface, AzureClientWithSasInterface, StatefulTriggerInterface {

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

    private Property<String> container;

    private Property<String> prefix;

    protected Property<String> regexp;

    protected Property<String> delimiter;

    private Property<ActionInterface.Action> action;

    private Copy.CopyObject moveTo;

    @Builder.Default
    private Property<ListInterface.Filter> filter = Property.ofValue(Filter.FILES);

    @Schema(
        title = "The maximum number of files to retrieve at once",
        description = "Limits the number of blobs retrieved per polling interval. If not specified, all matching blobs will be retrieved."
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<Integer> maxFiles = Property.ofValue(25);

    @Builder.Default
    private final Property<On> on = Property.ofValue(On.CREATE_OR_UPDATE);

    private Property<String> stateKey;

    private Property<Duration> stateTtl;

    @Builder.Default
    @lombok.Getter(lombok.AccessLevel.NONE)
    @lombok.ToString.Exclude
    private transient AtomicReference<Disposable> disposable = new AtomicReference<>();

    @Builder.Default
    @lombok.Getter(lombok.AccessLevel.NONE)
    @lombok.ToString.Exclude
    private transient AtomicBoolean killed = new AtomicBoolean(false);

    @Builder.Default
    @lombok.Getter(lombok.AccessLevel.NONE)
    @lombok.ToString.Exclude
    private transient AtomicReference<CountDownLatch> latchRef = new AtomicReference<>();

    @Override
    public void kill() {
        if (killed.compareAndSet(false, true)) {
            Disposable current = disposable.getAndSet(null);
            if (current != null) {
                try {
                    current.dispose();
                } catch (Exception ignored) {
                }
            }
            CountDownLatch latch = latchRef.get();
            if (latch != null) {
                latch.countDown();
            }
        }
    }

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        var runContext = conditionContext.getRunContext();
        var rOn = runContext.render(on).as(On.class).orElse(On.CREATE_OR_UPDATE);
        var rStateKey = runContext.render(stateKey).as(String.class).orElse(StatefulTriggerService.defaultKey(context.getNamespace(), context.getFlowId(), id));
        var rStateTtl = runContext.render(stateTtl).as(Duration.class);

        var asyncClient = BlobService.asyncClient(
            this.endpoint, this.connectionString, this.sharedKeyAccountName, this.sharedKeyAccountAccessKey, this.sasToken, runContext
        );
        var containerAsyncClient = asyncClient.getBlobContainerAsyncClient(runContext.render(this.container).as(String.class).orElseThrow());

        var options = new ListBlobsOptions().setPrefix(runContext.render(this.prefix).as(String.class).orElse(null));

        var rDelimiter = runContext.render(this.delimiter).as(String.class).orElse(null);
        Flux<BlobItem> flux = rDelimiter != null ? containerAsyncClient.listBlobsByHierarchy(rDelimiter, options) : containerAsyncClient.listBlobs(options);

        var renderedRegexp = runContext.render(this.regexp).as(String.class).orElse(null);
        var renderedFilter = runContext.render(this.filter).as(Filter.class).orElse(Filter.FILES);
        var rMaxFiles = runContext.render(this.maxFiles).as(Integer.class).orElse(25);

        var listLatch = new CountDownLatch(1);
        var error = new AtomicReference<Throwable>();
        var list = new java.util.ArrayList<Blob>();

        var listDisposable = flux
            .filter(item ->
            {
                boolean isDir = Boolean.TRUE.equals(item.isPrefix()) || (item.getProperties() != null && item.getProperties().getContentType() == null);
                if (renderedFilter == Filter.FILES && isDir)
                    return false;
                if (renderedFilter == Filter.DIRECTORY && !isDir)
                    return false;
                if (renderedRegexp != null && !item.getName().matches(renderedRegexp))
                    return false;
                return true;
            })
            .map(item -> Blob.of(containerAsyncClient.getBlobContainerName(), item))
            .take(rMaxFiles)
            .subscribe(
                list::add,
                err ->
                {
                    error.set(err);
                    listLatch.countDown();
                },
                listLatch::countDown
            );

        this.disposable.set(listDisposable);
        this.latchRef.set(listLatch);
        if (killed.get()) {
            this.kill();
        }
        try {
            listLatch.await();
        } finally {
            this.disposable.set(null);
            this.latchRef.set(null);
        }

        if (killed.get())
            throw new InterruptedException("Trigger was killed");
        if (error.get() != null) {
            if (error.get() instanceof Exception e)
                throw e;
            throw new Exception(error.get());
        }

        if (list.isEmpty()) {
            return Optional.empty();
        }

        var previousState = readState(runContext, rStateKey, rStateTtl);
        var actionBlobs = new java.util.ArrayList<Blob>();
        var toFire = new java.util.ArrayList<TriggeredBlob>();

        for (var blob : list) {
            if (killed.get())
                throw new InterruptedException("Trigger was killed");

            var uri = String.format("az://%s/%s", runContext.render(container).as(String.class).orElse(""), blob.getName());
            var modifiedAt = java.util.Optional.ofNullable(blob.getLastModified()).map(java.time.OffsetDateTime::toInstant).orElse(java.time.Instant.now());
            var version = java.util.Optional.ofNullable(blob.getETag()).orElse(String.valueOf(modifiedAt.toEpochMilli()));

            var candidate = StatefulTriggerService.Entry.candidate(uri, version, modifiedAt);
            var stateChange = computeAndUpdateState(previousState, candidate, rOn);

            if (stateChange.fire()) {
                var changeType = stateChange.isNew() ? ChangeType.CREATE : ChangeType.UPDATE;

                var tempFile = runContext.workingDir().createTempFile(io.kestra.core.utils.FileUtils.getExtension(blob.getName())).toFile();
                var dlLatch = new CountDownLatch(1);
                error.set(null);
                var propsRef = new AtomicReference<BlobProperties>();

                var blobAsyncClient = containerAsyncClient.getBlobAsyncClient(blob.getName());
                var dlDisposable = blobAsyncClient.downloadToFile(tempFile.getAbsolutePath(), true)
                    .subscribe(
                        props ->
                        {
                            propsRef.set(props);
                            dlLatch.countDown();
                        },
                        err ->
                        {
                            error.set(err);
                            dlLatch.countDown();
                        },
                        dlLatch::countDown
                    );
                this.disposable.set(dlDisposable);
                this.latchRef.set(dlLatch);
                if (killed.get()) {
                    this.kill();
                }
                try {
                    dlLatch.await();
                } finally {
                    this.disposable.set(null);
                    this.latchRef.set(null);
                }

                if (killed.get())
                    throw new InterruptedException("Trigger was killed");
                if (error.get() != null) {
                    if (error.get() instanceof Exception e)
                        throw e;
                    throw new Exception(error.get());
                }

                var blobProperties = propsRef.get();
                runContext.metric(Counter.of("file.size", blobProperties.getBlobSize()));

                var readFileUri = runContext.storage().putFile(tempFile);
                var downloadedBlob = blob.withUri(readFileUri);
                actionBlobs.add(blob);

                toFire.add(
                    TriggeredBlob.builder()
                        .blob(downloadedBlob)
                        .changeType(changeType)
                        .build()
                );
            }
        }

        writeState(runContext, rStateKey, previousState, rStateTtl);

        if (toFire.isEmpty()) {
            return Optional.empty();
        }

        if (killed.get())
            throw new InterruptedException("Trigger was killed");

        BlobService.archive(actionBlobs, runContext.render(this.action).as(ActionInterface.Action.class).orElse(null), this.moveTo, runContext, this, this);

        var output = Output.builder().blobs(toFire).build();
        var execution = TriggerService.generateExecution(this, conditionContext, context, output);

        return Optional.of(execution);
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "List of blobs that triggered the flow, each with its change type")
        private final java.util.List<TriggeredBlob> blobs;
    }

    @Getter
    @AllArgsConstructor
    @Builder
    public static class TriggeredBlob {
        @JsonUnwrapped
        private final Blob blob;
        private final Trigger.ChangeType changeType;
    }

    public enum ChangeType {
        CREATE,
        UPDATE
    }
}
