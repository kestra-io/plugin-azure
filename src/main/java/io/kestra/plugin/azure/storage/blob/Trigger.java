package io.kestra.plugin.azure.storage.blob;

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
import java.util.Optional;
import java.util.stream.Collectors;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Run a flow as soon as files are uploaded to Azure Blob Storage",
    description = "This trigger will poll the specified Azure Blob Storage bucket every `interval`. " +
    "Using the `from` and `regExp` properties, you can define which files arrival will trigger the flow. " +
    "Under the hood, we use the Azure Blob Storage API to list the files in a specified location and download them to the internal storage and process them with the declared `action`. " +
    "You can use the `action` property to move or delete the files from the container after processing to avoid the trigger to be fired again for the same files during the next polling interval."
)
@Plugin(
    examples = {
        @Example(
            title = "Run a flow if one or more files arrived in the specified Azure Blob Storage bucket location. Then, process all files in a for-loop either sequentially or concurrently, depending on the `concurrencyLimit` property.",
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
            title = "Run a flow whenever one or more files arrived in the specified Azure Blob Storage bucket location. Then, process files and delete processed files to avoid re-triggering the flow for the same Blob objects during the next polling interval.",
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
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<List.Output>, AbstractConnectionInterface, ListInterface, ActionInterface, AbstractBlobStorageContainerInterface, AzureClientWithSasInterface {

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
    private Property<ListInterface.Filter> filter = Property.of(Filter.FILES);

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

        if (run.getBlobs().isEmpty()) {
            return Optional.empty();
        }

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
                    .name(Property.of(object.getName()))
                    .build();
                Download.Output downloadOutput = download.run(runContext);

                return object.withUri(downloadOutput.getBlob().getUri());
            }))
            .collect(Collectors.toList());


        BlobService.archive(
            run.getBlobs(),
            runContext.render(this.action).as(ActionInterface.Action.class).orElse(null),
            this.moveTo,
            runContext,
            this,
            this
        );

        Execution execution = TriggerService.generateExecution(this, conditionContext, context, List.Output.builder().blobs(list).build());

        return Optional.of(execution);
    }
}
