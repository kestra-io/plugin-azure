package io.kestra.plugin.azure.aifoundry;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.azure.ai.projects.AIProjectClientBuilder;
import com.azure.ai.projects.EvaluationsClient;
import com.azure.ai.projects.models.Evaluation;
import com.azure.core.credential.TokenCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.models.triggers.StatefulTriggerInterface;
import io.kestra.core.models.triggers.StatefulTriggerService;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.models.triggers.TriggerService;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import static io.kestra.core.models.triggers.StatefulTriggerService.computeAndUpdateState;
import static io.kestra.core.models.triggers.StatefulTriggerService.readState;
import static io.kestra.core.models.triggers.StatefulTriggerService.writeState;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Fire an execution when an Azure AI Foundry evaluation completes",
            code = """
                    id: azure_ai_on_evaluation_complete
                    namespace: company.team
                    tasks:
                      - id: notify
                        type: io.kestra.plugin.core.log.Log
                        message: "Evaluation {{ trigger.evaluation.name }} finished with status {{ trigger.evaluation.status }}"
                    triggers:
                      - id: on_evaluation
                        type: io.kestra.plugin.azure.aifoundry.Trigger
                        endpoint: "{{ secret('AZURE_AI_FOUNDRY_ENDPOINT') }}"
                        interval: PT5M
                """
        )
    }
)
@Schema(
    title = "Poll Azure AI Foundry for completed evaluations",
    description = "Polls the Azure AI Projects EvaluationsClient for terminal evaluations. " +
        "Fires an execution when a newly observed evaluation reaches one of the configured terminal statuses. " +
        "The statuses and maximum number of evaluations inspected per poll are configurable. " +
        "Subsequent polls will not re-fire for already-observed evaluations because trigger state deduplicates " +
        "evaluation names and statuses; state retention can be controlled with stateTtl."
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Trigger.Output>, StatefulTriggerInterface {

    @Schema(
        title = "Azure AI Foundry endpoint",
        description = "The Azure AI Foundry project endpoint URL."
    )
    @NotNull
    @PluginProperty(group = "connection")
    private Property<String> endpoint;

    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    @Schema(title = "Statuses", description = "Evaluation statuses that should trigger executions. Defaults to Completed, Failed, Canceled, and Expired.")
    @Builder.Default
    @PluginProperty(group = "main")
    private Property<@NotEmpty List<String>> statuses = Property.ofValue(List.of("Completed", "Failed", "Canceled", "Expired"));

    @Schema(title = "Maximum evaluations", description = "Maximum number of recent evaluations to inspect per polling interval. Defaults to 25.")
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<@Min(1) Integer> maxEvaluations = Property.ofValue(25);

    @Builder.Default
    @Schema(title = "State change mode", description = "Stateful trigger change mode used for observed evaluations.")
    @PluginProperty(group = "advanced")
    private final Property<On> on = Property.ofValue(On.CREATE);

    @Schema(title = "State key", description = "Custom key used to store observed evaluations.")
    @PluginProperty(group = "advanced")
    private Property<String> stateKey;

    @Schema(title = "State TTL", description = "How long observed evaluation state is retained.")
    @PluginProperty(group = "advanced")
    private Property<Duration> stateTtl;

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();

        String rStateKey = runContext.render(stateKey).as(String.class).orElse(StatefulTriggerService.defaultKey(context.getNamespace(), context.getFlowId(), id));
        Optional<Duration> rStateTtl = runContext.render(stateTtl).as(Duration.class);
        On rOn = runContext.render(on).as(On.class).orElse(On.CREATE);
        List<String> rStatuses = runContext.render(statuses).asList(String.class);
        int rMaxEvaluations = runContext.render(maxEvaluations).as(Integer.class).orElse(25);

        if (rStatuses.isEmpty()) {
            throw new IllegalArgumentException("statuses cannot be empty");
        }
        if (rMaxEvaluations < 1) {
            throw new IllegalArgumentException("maxEvaluations must be greater than or equal to 1");
        }

        String endpointStr = runContext.render(this.endpoint).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("endpoint is required"));

        TokenCredential credential = new DefaultAzureCredentialBuilder().build();

        EvaluationsClient evalClient = new AIProjectClientBuilder()
            .endpoint(endpointStr)
            .credential(credential)
            .buildEvaluationsClient();

        List<EvaluationRecord> newEvaluations = new ArrayList<>();
        var previousState = readState(runContext, rStateKey, rStateTtl);

        int count = 0;
        for (Evaluation evaluation : evalClient.listEvaluations()) {
            if (count >= rMaxEvaluations) {
                break;
            }
            count++;

            String status = evaluation.getStatus();

            if (rStatuses.stream().anyMatch(s -> s.equalsIgnoreCase(status))) {
                var candidate = StatefulTriggerService.Entry.candidate(evaluation.getName(), status, Instant.now());
                var stateChange = computeAndUpdateState(previousState, candidate, rOn);

                if (stateChange.fire()) {
                    newEvaluations.add(
                        EvaluationRecord.builder()
                            .name(evaluation.getName())
                            .status(status)
                            .build()
                    );
                    runContext.logger().info("New evaluation observed: {} (status: {})", evaluation.getName(), status);
                }
            }
        }

        writeState(runContext, rStateKey, previousState, rStateTtl);

        if (newEvaluations.isEmpty()) {
            return Optional.empty();
        }

        Output output = Output.builder()
            .evaluation(newEvaluations.getFirst())
            .evaluations(newEvaluations)
            .total(newEvaluations.size())
            .build();

        return Optional.ofNullable(
            TriggerService.generateExecution(this, conditionContext, context, output)
        );
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "First completed evaluation")
        private EvaluationRecord evaluation;

        @Schema(title = "List of all newly completed evaluations")
        private List<EvaluationRecord> evaluations;

        @Schema(title = "Total number of newly completed evaluations")
        private Integer total;
    }

    @Builder
    @Getter
    public static class EvaluationRecord {
        @Schema(title = "Evaluation name")
        private String name;

        @Schema(title = "Evaluation status")
        private String status;
    }
}
