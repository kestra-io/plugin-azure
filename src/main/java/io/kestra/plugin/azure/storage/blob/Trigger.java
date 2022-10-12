package io.kestra.plugin.azure.storage.blob;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.executions.ExecutionTrigger;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.azure.AbstractConnectionInterface;
import io.kestra.plugin.azure.storage.abstracts.AbstractStorageInterface;
import io.kestra.plugin.azure.storage.blob.abstracts.AbstractBlobStorageContainerInterface;
import io.kestra.plugin.azure.storage.blob.abstracts.ActionInterface;
import io.kestra.plugin.azure.storage.blob.abstracts.ListInterface;
import io.kestra.plugin.azure.storage.blob.models.Blob;
import io.kestra.plugin.azure.storage.blob.services.BlobService;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.time.Duration;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Wait for files on Azure Blob Storage",
    description = "This trigger will poll every `interval` blob storage. " +
        "You can search for all files in a container or directory in `from` or you can filter the files with a `regExp`." +
        "The detection is atomic, internally we do a list and interact only with files listed.\n" +
        "Once a file is detected, we download the file on internal storage and processed with declared `action` " +
        "in order to move or delete the files from the container (to avoid double detection on new poll)"
)
@Plugin(
    examples = {
        @Example(
            title = "Wait for a list of file on a Azure Blob Storage bucket and iterate through the files",
            full = true,
            code = {
                "id: storage-listen",
                "namespace: io.kestra.tests",
                "",
                "tasks:",
                "  - id: each",
                "    type: io.kestra.core.tasks.flows.EachSequential",
                "    tasks:",
                "      - id: return",
                "        type: io.kestra.core.tasks.debugs.Return",
                "        format: \"{{taskrun.value}}\"",
                "    value: \"{{ trigger.blobs | jq('.[].uri') }}\"",
                "",
                "triggers:",
                "  - id: watch",
                "    type: io.kestra.plugin.azure.storage.blob.Trigger",
                "    endpoint: \"https://yourblob.blob.core.windows.net\"",
                "    connectionString: \"DefaultEndpointsProtocol=...==\"",
                "    container: \"mydata\"",
                "    prefix: \"trigger/storage-listen\"",
                "    action: MOVE",
                "    moveTo: ",
                "      container: mydata",
                "      name: archive",
            }
        )
    }
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<List.Output>, AbstractConnectionInterface, ListInterface, AbstractBlobStorageContainerInterface, AbstractStorageInterface {
    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    protected String endpoint;

    protected String connectionString;

    protected String sharedKeyAccountName;

    protected String sharedKeyAccountAccessKey;

    protected String sasToken;

    private String container;

    private String prefix;

    protected String regexp;

    protected String delimiter;

    private ActionInterface.Action action;

    private Copy.CopyObject moveTo;

    @Builder.Default
    private ListInterface.Filter filter = Filter.FILES;

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
            .container(this.container)
            .prefix(this.prefix)
            .delimiter(this.delimiter)
            .regexp(this.regexp)
            .delimiter(this.delimiter)
            .build();
        List.Output run = task.run(runContext);

        if (run.getBlobs().size() == 0) {
            return Optional.empty();
        }

        String executionId = IdUtils.create();

        java.util.List<Blob> list = run
            .getBlobs()
            .stream()
            .map(throwFunction(object -> {
                Download download = Download.builder()
                    .id(this.id)
                    .type(List.class.getName())
                    .endpoint(this.endpoint)
                    .connectionString(this.connectionString)
                    .sharedKeyAccountName(this.sharedKeyAccountName)
                    .sharedKeyAccountAccessKey(this.sharedKeyAccountAccessKey)
                    .sasToken(this.sasToken)
                    .container(this.container)
                    .name(object.getName())
                    .build();
                Download.Output downloadOutput = download.run(runContext);

                return object.withUri(downloadOutput.getBlob().getUri());
            }))
            .collect(Collectors.toList());


        BlobService.archive(
            run.getBlobs(),
            this.action,
            this.moveTo,
            runContext,
            this,
            this
        );

        ExecutionTrigger executionTrigger = ExecutionTrigger.of(
            this,
            List.Output.builder().blobs(list).build()
        );

        Execution execution = Execution.builder()
            .id(executionId)
            .namespace(context.getNamespace())
            .flowId(context.getFlowId())
            .flowRevision(context.getFlowRevision())
            .state(new State())
            .trigger(executionTrigger)
            .build();

        return Optional.of(execution);
    }
}
