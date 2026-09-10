package io.kestra.plugin.azure.ml;

import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import com.azure.core.management.exception.ManagementException;
import com.azure.resourcemanager.machinelearning.MachineLearningManager;
import com.azure.resourcemanager.machinelearning.models.ComputeInstance;
import com.azure.resourcemanager.machinelearning.models.ComputeInstanceState;
import com.azure.resourcemanager.machinelearning.models.ComputeResource;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.Await;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * Shared start/stop lifecycle for Azure Machine Learning compute instances, used by {@link StartComputeInstance}
 * and {@link StopComputeInstance}.
 */
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractComputeInstanceLifecycle extends AbstractMachineLearningTask implements RunnableTask<AbstractComputeInstanceLifecycle.Output> {
    @Schema(title = "Compute instance name")
    @NotNull
    @PluginProperty(group = "main")
    protected Property<String> computeName;

    @Schema(title = "Wait for the target state", description = "If true (default), poll until the compute instance actually reaches the target state")
    @Builder.Default
    @PluginProperty(group = "execution")
    protected Property<Boolean> wait = Property.ofValue(Boolean.TRUE);

    @Schema(title = "Polling frequency", description = "Interval and max duration used when `wait=true`")
    @Builder.Default
    @PluginProperty(group = "advanced")
    protected ComputeCheckFrequency checkFrequency = ComputeCheckFrequency.builder().build();

    protected abstract String actionVerb();

    protected abstract ComputeInstanceState targetState();

    protected abstract void invokeAction(MachineLearningManager manager, String resourceGroupName, String workspaceName, String computeName);

    private static boolean isFailureState(ComputeInstanceState state) {
        return state == ComputeInstanceState.CREATE_FAILED
            || state == ComputeInstanceState.SETUP_FAILED
            || state == ComputeInstanceState.USER_SETUP_FAILED
            || state == ComputeInstanceState.UNUSABLE;
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        var logger = runContext.logger();
        String rResourceGroupName = rResourceGroupName(runContext);
        String rWorkspaceName = rWorkspaceName(runContext);
        String rComputeName = runContext.render(this.computeName).as(String.class).orElseThrow();

        MachineLearningManager manager = machineLearningManager(runContext);

        ComputeResource compute;
        try {
            compute = manager.computes().get(rResourceGroupName, rWorkspaceName, rComputeName);
        } catch (ManagementException e) {
            if (e.getResponse() != null && e.getResponse().getStatusCode() == 404) {
                throw new IllegalArgumentException("Compute instance '%s' was not found in workspace '%s'".formatted(rComputeName, rWorkspaceName), e);
            }
            throw e;
        }

        if (!(compute.properties() instanceof ComputeInstance computeInstance)) {
            throw new IllegalArgumentException("Compute '%s' is not a compute instance — this task only supports compute instances, not compute clusters".formatted(rComputeName));
        }

        ComputeInstanceState currentState = computeInstance.properties() != null ? computeInstance.properties().state() : null;
        if (currentState == targetState()) {
            logger.info("Compute instance '{}' is already {}", rComputeName, targetState().toString().toLowerCase());
            return Output.builder().computeName(rComputeName).state(currentState.toString()).build();
        }

        try {
            invokeAction(manager, rResourceGroupName, rWorkspaceName, rComputeName);
        } catch (ManagementException e) {
            if (e.getResponse() != null && e.getResponse().getStatusCode() == 409) {
                throw new IllegalStateException(
                    "Could not %s compute instance '%s' — it is likely already transitioning between states (e.g. still stopping); wait for it to settle and retry".formatted(actionVerb(), rComputeName), e
                );
            }
            throw e;
        }
        logger.info("Requested to {} compute instance '{}'", actionVerb(), rComputeName);

        if (!Boolean.TRUE.equals(runContext.render(this.wait).as(Boolean.class).orElseThrow())) {
            return Output.builder().computeName(rComputeName).state(null).build();
        }

        Duration interval = runContext.render(this.checkFrequency.getInterval()).as(Duration.class).orElseThrow();
        Duration maxDuration = runContext.render(this.checkFrequency.getMaxDuration()).as(Duration.class).orElseThrow();

        // Await.until checks the timeout only between sleeps, not during one — capped the same way as the job
        // poll loop in MachineLearningService.awaitTerminalState, and for the same reason.
        Duration pollInterval = interval.compareTo(maxDuration) > 0 ? maxDuration : interval;

        AtomicReference<ComputeInstanceState> lastState = new AtomicReference<>(currentState);
        try {
            Await.until(
                () ->
                {
                    ComputeResource refreshed;
                    try {
                        refreshed = manager.computes().get(rResourceGroupName, rWorkspaceName, rComputeName);
                    } catch (ManagementException e) {
                        // A single transient ARM error must not fail a wait that can span minutes to hours — log
                        // and keep polling; a persistent problem still surfaces via the timeout below.
                        logger.warn("Transient error polling compute instance '{}' status, will retry: {}", rComputeName, e.getMessage());
                        return false;
                    }
                    ComputeInstanceState state = refreshed.properties() instanceof ComputeInstance ci && ci.properties() != null ? ci.properties().state() : null;
                    lastState.set(state);
                    if (isFailureState(state)) {
                        // A definitive failure is already known — don't burn the rest of maxDuration waiting for
                        // a state that will never arrive.
                        throw new IllegalStateException("Compute instance '%s' reached failure state '%s' while waiting for '%s'".formatted(rComputeName, state, targetState()));
                    }
                    return state == targetState();
                },
                pollInterval,
                maxDuration
            );
        } catch (TimeoutException e) {
            throw new IllegalStateException(
                "Compute instance '%s' did not reach state '%s' within %s; last observed state was '%s'".formatted(rComputeName, targetState(), maxDuration, lastState.get())
            );
        }

        logger.info("Compute instance '{}' is now {}", rComputeName, lastState.get());

        return Output.builder()
            .computeName(rComputeName)
            .state(lastState.get() != null ? lastState.get().toString() : null)
            .build();
    }

    @SuperBuilder
    @Getter
    @NoArgsConstructor
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Compute instance name")
        private String computeName;

        @Schema(title = "Compute instance state", description = "Observed state after the action; null when `wait=false`")
        private String state;
    }
}
