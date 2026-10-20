package io.kestra.plugin.azure.devops.pipelines;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.shared.AzureIdentityConnectionInterface;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import static io.kestra.core.models.triggers.StatefulTriggerService.*;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            title = "React when an Azure DevOps pipeline run completes",
            full = true,
            code = """
                id: azure_devops_run_completed
                namespace: company.team

                triggers:
                  - id: on_run_completed
                    type: io.kestra.plugin.azure.devops.pipelines.Trigger
                    organization: my-org
                    project: my-project
                    pipelineId: 42
                    personalAccessToken: "{{ secret('AZURE_DEVOPS_PAT') }}"
                    interval: PT1M

                tasks:
                  - id: handle_event
                    type: io.kestra.plugin.core.log.Log
                    message: "Run {{ trigger.id }} completed with {{ trigger.result }}"
                """
        )
    }
)
@Schema(
    title = "Trigger flows on completed Azure DevOps pipeline runs",
    description = """
        Periodically polls an Azure DevOps pipeline and starts an execution for newly observed completed runs.
        Maintains an exactly-once watermark in Kestra's namespace KV store.
        """
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Trigger.Output>, StatefulTriggerInterface, AzureIdentityConnectionInterface {
    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    @Schema(
        title = "Azure DevOps organization",
        description = """
            The name of the Azure DevOps organization or Azure DevOps Server collection.
            """
    )
    @NotNull
    @PluginProperty(group = "connection")
    protected Property<String> organization;

    @Schema(
        title = "Azure DevOps project",
        description = """
            The name or ID of the Azure DevOps project containing the pipeline.
            """
    )
    @NotNull
    @PluginProperty(group = "main")
    protected Property<String> project;

    @Schema(
        title = "Pipeline ID",
        description = """
            The ID of the pipeline to monitor.
            """
    )
    @NotNull
    @PluginProperty(group = "main")
    protected Property<Integer> pipelineId;

    @Schema(
        title = "Base URL",
        description = """
            Base URL of the Azure DevOps instance. Defaults to `https://dev.azure.com`.
            """
    )
    @Builder.Default
    @PluginProperty(group = "connection")
    protected Property<String> baseUrl = Property.ofValue("https://dev.azure.com");

    @Schema(
        title = "Personal Access Token",
        description = """
            Azure DevOps Personal Access Token (PAT) with `Build: Read & execute` scope.
            """
    )
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    @PluginProperty(group = "connection", secret = true)
    protected Property<String> personalAccessToken;

    @Schema(
        title = "Azure tenant ID",
        description = """
            Azure Entra tenant ID used for service principal authentication.
            """
    )
    @PluginProperty(group = "connection")
    protected Property<String> tenantId;

    @Schema(
        title = "Azure client ID",
        description = """
            Client ID of the Azure app registration.
            """
    )
    @PluginProperty(group = "connection")
    protected Property<String> clientId;

    @Schema(
        title = "Azure client secret",
        description = """
            Client secret of the Azure app registration.
            """
    )
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    @PluginProperty(group = "connection", secret = true)
    protected Property<String> clientSecret;

    @Schema(
        title = "PEM certificate",
        description = """
            PEM certificate content for certificate-based authentication.
            """
    )
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    @PluginProperty(group = "connection", secret = true)
    protected Property<String> pemCertificate;

    @Schema(
        title = "Results filter",
        description = """
            Run results that should trigger executions. If not specified, all completed runs trigger executions.
            """
    )
    @PluginProperty(group = "main")
    private Property<List<String>> results;

    @Builder.Default
    @Schema(
        title = "State change mode",
        description = """
            Stateful trigger change mode used for observed pipeline runs. Defaults to `CREATE`.
            """
    )
    @PluginProperty(group = "advanced")
    private final Property<On> on = Property.ofValue(On.CREATE);

    @Schema(
        title = "Maximum runs to inspect",
        description = """
            Maximum number of recent runs to inspect per polling interval. Defaults to 25.
            """
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<Integer> maxRuns = Property.ofValue(25);

    @Schema(
        title = "State key",
        description = """
            Custom key used to store observed run watermarks in the KV store.
            """
    )
    @PluginProperty(group = "advanced")
    private Property<String> stateKey;

    @Schema(
        title = "State TTL",
        description = """
            How long observed pipeline run state is retained in the KV store.
            """
    )
    @PluginProperty(group = "advanced")
    private Property<Duration> stateTtl;

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Integer pId = runContext.render(this.pipelineId).as(Integer.class).orElseThrow();
        String defaultWatermarkKey = "watermark_" + context.getNamespace() + "_" + context.getFlowId() + "_" + this.getId() + "_p" + pId;
        String rState = runContext.render(stateKey).as(String.class).orElse(defaultWatermarkKey);
        Optional<Duration> rTtl = runContext.render(stateTtl).as(Duration.class);
        List<String> rResults = runContext.render(results).asList(String.class);

        ListRuns.Output listedRuns = ListRuns.builder()
            .id(this.id)
            .type(ListRuns.class.getName())
            .organization(this.organization)
            .project(this.project)
            .pipelineId(this.pipelineId)
            .baseUrl(this.baseUrl)
            .personalAccessToken(this.personalAccessToken)
            .tenantId(this.tenantId)
            .clientId(this.clientId)
            .clientSecret(this.clientSecret)
            .pemCertificate(this.pemCertificate)
            .state(Property.ofValue("completed"))
            .maxRuns(this.maxRuns)
            .build()
            .run(runContext);

        var previousState = readState(runContext, rState, rTtl);
        On rOn = runContext.render(on).as(On.class).orElse(On.CREATE);

        List<PipelineRun> newRuns = new ArrayList<>();
        for (PipelineRun run : listedRuns.getRuns()) {
            if (rResults.isEmpty() || rResults.stream().anyMatch(res -> res.equalsIgnoreCase(run.getResult()))) {
                Instant modifiedAt = Optional.ofNullable(run.getFinishedDate()).orElseGet(Instant::now);
                String outcome = run.getResult() != null ? run.getResult() : run.getState();

                var candidate = StatefulTriggerService.Entry.candidate(String.valueOf(run.getId()), outcome, modifiedAt);
                var stateChange = computeAndUpdateState(previousState, candidate, rOn);

                if (stateChange.fire()) {
                    newRuns.add(run);
                }
            }
        }

        writeState(runContext, rState, previousState, rTtl);

        if (newRuns.isEmpty()) {
            return Optional.empty();
        }

        PipelineRun firstRun = newRuns.get(0);
        return Optional.of(
            TriggerService.generateExecution(
                this,
                conditionContext,
                context,
                Output.builder()
                    .run(firstRun)
                    .runs(newRuns)
                    .total(newRuns.size())
                    .id(firstRun.getId())
                    .name(firstRun.getName())
                    .state(firstRun.getState())
                    .result(firstRun.getResult())
                    .webUrl(firstRun.getWebUrl())
                    .build()
            )
        );
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "First pipeline run",
            description = """
                The primary newly completed pipeline run triggering this execution.
                """
        )
        private final PipelineRun run;

        @Schema(
            title = "Pipeline runs",
            description = """
                All newly completed pipeline runs observed in this polling cycle.
                """
        )
        private final List<PipelineRun> runs;

        @Schema(
            title = "Total runs",
            description = """
                Total number of newly observed completed pipeline runs.
                """
        )
        private final Integer total;

        @Schema(
            title = "Run ID",
            description = """
                The unique identifier of the triggered pipeline run.
                """
        )
        private final Integer id;

        @Schema(
            title = "Run name",
            description = """
                The name or build number of the triggered pipeline run.
                """
        )
        private final String name;

        @Schema(
            title = "Run state",
            description = """
                The state of the triggered pipeline run (completed).
                """
        )
        private final String state;

        @Schema(
            title = "Run result",
            description = """
                The outcome of the triggered pipeline run (e.g. succeeded, failed).
                """
        )
        private final String result;

        @Schema(
            title = "Web URL",
            description = """
                Azure DevOps browser URL to view the triggered pipeline run.
                """
        )
        private final String webUrl;
    }
}
