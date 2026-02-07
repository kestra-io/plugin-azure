package io.kestra.plugin.azure.storage.blob;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.AbstractConnectionInterface;
import io.kestra.plugin.azure.AzureClientWithSasInterface;
import io.kestra.plugin.azure.storage.blob.abstracts.AbstractBlobStorageContainerInterface;
import io.kestra.plugin.azure.storage.blob.abstracts.ActionInterface;
import io.kestra.plugin.azure.storage.blob.abstracts.ListInterface;
import io.kestra.plugin.azure.storage.blob.models.Blob;
import io.kestra.plugin.azure.storage.blob.services.BlobService;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Optional;
import java.util.stream.Stream;

import static io.kestra.core.models.triggers.StatefulTriggerService.*;
import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@NoArgsConstructor
@Getter
@ToString
@EqualsAndHashCode

@Schema(
    title = "Trigger a flow on a new file arrival in an Azure Blob Storage container.",
    description = "This trigger will poll the specified Azure Blob Storage container every `interval`. "
        + "Using the `from` and `regExp` properties, you can define which files' arrival will trigger the flow. "
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
                    type: io.kestra.plugin.core.flow.ForEach
                    concurrencyLimit: 1
                    values: "{{ trigger.blobs | jq('.[].uri') }}"
                    tasks:
                      - id: return
                        type: io.kestra.plugin.core.debug.Return
                        format: "{{ taskrun.value }}"

                triggers:
                  - id: watch
                    type: io.kestra.plugin.azure.storage.blob.Trigger
                    interval: PT5M
                    endpoint: "https://yourblob.blob.core.windows.net"
                    connectionString: "DefaultEndpointsProtocol=...=="
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
                    type: io.kestra.plugin.core.flow.ForEach
                    values: "{{ trigger.blobs | jq('.[].name') }}"
                    tasks:
                      - id: return
                        type: io.kestra.plugin.core.debug.Return
                        format: "{{ taskrun.value }}"

                      - id: delete
                        type: io.kestra.plugin.azure.storage.blob.Delete
                        endpoint: "https://yourblob.blob.core.windows.net"
                        connectionString: "DefaultEndpointsProtocol=...=="
                        container: myBlobContainer
                        name: "{{ taskrun.value }}"

                triggers:
                  - id: watch
                    type: io.kestra.plugin.azure.storage.blob.Trigger
                    endpoint: "https://yourblob.blob.core.windows.net"
                    connectionString: "DefaultEndpointsProtocol=...=="
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
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Trigger.Output>, AbstractConnectionInterface, ListInterface, ActionInterface, AbstractBlobStorageContainerInterface, AzureClientWithSasInterface, StatefulTriggerInterface {

    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    protected Property<String> endpoint;

    protected Property<String> connectionString;

    protected Property<String> sharedKeyAccountName;

    protected Property<String> sharedKeyAccountAccessKey;

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
    private Property<Integer> maxFiles = Property.ofValue(25);

    @Builder.Default
    private final Property<On> on = Property.ofValue(On.CREATE_OR_UPDATE);

    private Property<String> stateKey;

    private Property<Duration> stateTtl;

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        var rOn = runContext.render(on).as(On.class).orElse(On.CREATE_OR_UPDATE);
        var rStateKey = runContext.render(stateKey).as(String.class).orElse(StatefulTriggerService.defaultKey(context.getNamespace(), context.getFlowId(), id));
        var rStateTtl = runContext.render(stateTtl).as(Duration.class);

        var task = List.builder()
            .id(this.id)
            .type(List.class.getName())
            .endpoint(this.endpoint)
            .connectionString(this.connectionString)
            .sharedKeyAccountName(this.sharedKeyAccountName)
            .sharedKeyAccountAccessKey(this.sharedKeyAccountAccessKey)
            .sasToken(this.sasToken)
            .container(this.container)
            .prefix(this.prefix)
            .delimiter(this.delimiter)
            .regexp(this.regexp)
            .delimiter(this.delimiter)
            .maxFiles(this.maxFiles)
            .build();
        List.Output run = task.run(runContext);

        if (run.getBlobs().isEmpty()) {
            return Optional.empty();
        }

        var previousState = readState(runContext, rStateKey, rStateTtl);

        var actionBlobs = new ArrayList<Blob>();

        var toFire = run.getBlobs().stream()
            .flatMap(throwFunction(blob -> {
                var uri = String.format("az://%s/%s", runContext.render(container).as(String.class).orElse(""), blob.getName());
                var modifiedAt = Optional.ofNullable(blob.getLastModified()).map(java.time.OffsetDateTime::toInstant).orElse(Instant.now());
                var version = Optional.ofNullable(blob.getETag()).orElse(String.valueOf(modifiedAt.toEpochMilli()));

                var candidate = StatefulTriggerService.Entry.candidate(uri, version, modifiedAt);

                var stateChange = computeAndUpdateState(previousState, candidate, rOn);

                if (stateChange.fire()) {
                    var changeType = stateChange.isNew() ? ChangeType.CREATE : ChangeType.UPDATE;

                    Download download = Download.builder()
                        .id(this.id)
                        .type(Download.class.getName())
                        .endpoint(this.endpoint)
                        .connectionString(this.connectionString)
                        .sharedKeyAccountName(this.sharedKeyAccountName)
                        .sharedKeyAccountAccessKey(this.sharedKeyAccountAccessKey)
                        .sasToken(this.sasToken)
                        .container(this.container)
                        .name(Property.ofValue(blob.getName()))
                        .build();

                    Download.Output downloadOutput = download.run(runContext);
                    Blob downloadedBlob = blob.withUri(downloadOutput.getBlob().getUri());
                    actionBlobs.add(blob);

                    return Stream.of(TriggeredBlob.builder()
                        .blob(downloadedBlob)
                        .changeType(changeType)
                        .build());
                }

                return Stream.empty();
            }))
            .toList();

        writeState(runContext, rStateKey, previousState, rStateTtl);

        if (toFire.isEmpty()) {
            return Optional.empty();
        }

        BlobService.archive(actionBlobs, runContext.render(this.action).as(ActionInterface.Action.class).orElse(null), this.moveTo, runContext, this, this);

        var output = Output.builder().blobs(toFire).build();
        Execution execution = TriggerService.generateExecution(this, conditionContext, context, output);

        return Optional.of(execution);
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "List of blobs that triggered the flow, each with its change type.")
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
