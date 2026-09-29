package io.kestra.plugin.azure.ml;

import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import com.azure.core.management.exception.ManagementException;
import com.azure.resourcemanager.machinelearning.MachineLearningManager;
import com.azure.resourcemanager.machinelearning.models.AmlCompute;
import com.azure.resourcemanager.machinelearning.models.ClusterUpdateParameters;
import com.azure.resourcemanager.machinelearning.models.ComputeResource;
import com.azure.resourcemanager.machinelearning.models.ProvisioningState;
import com.azure.resourcemanager.machinelearning.models.ScaleSettings;
import com.azure.resourcemanager.machinelearning.models.ScaleSettingsInformation;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.Await;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: azure_ml_scale_cluster
                namespace: company.team

                tasks:
                  - id: scale_up
                    type: io.kestra.plugin.azure.ml.ScaleCluster
                    tenantId: "{{ secret('AZURE_TENANT_ID') }}"
                    clientId: "{{ secret('AZURE_CLIENT_ID') }}"
                    clientSecret: "{{ secret('AZURE_CLIENT_SECRET') }}"
                    subscriptionId: "{{ secret('AZURE_SUBSCRIPTION_ID') }}"
                    resourceGroupName: ml-rg
                    workspaceName: ml-workspace
                    computeName: gpu-cluster
                    minNodeCount: 1
                    maxNodeCount: 4
                """
        )
    }
)
@Schema(
    title = "Update the autoscale settings of an Azure Machine Learning compute cluster",
    description = "Sets the min/max node count (and optional idle scale-down delay) of an existing AmlCompute cluster. Only compute clusters support autoscaling; compute instances do not."
)
public class ScaleCluster extends AbstractMachineLearningTask implements RunnableTask<ScaleCluster.Output> {
    @Schema(title = "Compute cluster name")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> computeName;

    @Schema(title = "Minimum node count", description = "Minimum number of nodes kept available; 0 (default) scales the cluster down to no nodes when idle")
    @lombok.Builder.Default
    @PluginProperty(group = "main")
    private Property<@Min(0) @Max(1000) Integer> minNodeCount = Property.ofValue(0);

    @Schema(title = "Maximum node count", description = "Maximum number of nodes the cluster can scale out to")
    @NotNull
    @PluginProperty(group = "main")
    private Property<@Min(1) @Max(1000) Integer> maxNodeCount;

    @Schema(title = "Node idle time before scale down", description = "How long a node stays idle before being deallocated; defaults to Azure's own setting when not provided")
    @PluginProperty(group = "advanced")
    private Property<Duration> nodeIdleTimeBeforeScaleDown;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var logger = runContext.logger();
        String rResourceGroupName = rResourceGroupName(runContext);
        String rWorkspaceName = rWorkspaceName(runContext);
        String rComputeName = runContext.render(this.computeName).as(String.class).orElseThrow();
        int rMinNodeCount = runContext.render(this.minNodeCount).as(Integer.class).orElse(0);
        int rMaxNodeCount = runContext.render(this.maxNodeCount).as(Integer.class).orElseThrow();

        MachineLearningManager manager = machineLearningManager(runContext);

        ComputeResource compute;
        try {
            compute = manager.computes().get(rResourceGroupName, rWorkspaceName, rComputeName);
        } catch (ManagementException e) {
            if (e.getResponse() != null && e.getResponse().getStatusCode() == 404) {
                throw new IllegalArgumentException("Compute cluster '%s' was not found in workspace '%s'".formatted(rComputeName, rWorkspaceName), e);
            }
            throw e;
        }

        if (!(compute.properties() instanceof AmlCompute)) {
            throw new IllegalArgumentException("Compute '%s' is not a compute cluster (AmlCompute) — only compute clusters support autoscaling, not compute instances".formatted(rComputeName));
        }

        ScaleSettings scaleSettings = new ScaleSettings()
            .withMinNodeCount(rMinNodeCount)
            .withMaxNodeCount(rMaxNodeCount);
        runContext.render(this.nodeIdleTimeBeforeScaleDown).as(Duration.class).ifPresent(scaleSettings::withNodeIdleTimeBeforeScaleDown);

        // Neither the fluent ComputeResource$Update.apply() nor the lower-level ComputesClient.beginUpdate(...)
        // .getFinalResult() can be trusted to block until this specific endpoint's update actually completes: both
        // throw on the 202-Accepted interim response this operation returns, even though the underlying PATCH
        // request Azure receives succeeds and lands the requested scale settings regardless (confirmed live against
        // a real cluster, on both attempts — most likely the initial 202 for this operation is missing or has a
        // malformed Azure-AsyncOperation/Location header that azure-core's default LRO poll strategy needs to keep
        // polling, so the SDK surfaces it as a terminal error instead). So beginUpdate(...) is only used to fire the
        // request — its poller is never awaited — and completion is instead confirmed the same way
        // AbstractComputeInstanceLifecycle already does: by manually polling the resource's own GET endpoint until
        // its provisioning state reaches a terminal value.
        try {
            MachineLearningService.withTimeout(
                () -> manager.serviceClient().getComputes()
                    .beginUpdate(
                        rResourceGroupName, rWorkspaceName, rComputeName, new ClusterUpdateParameters().withProperties(new ScaleSettingsInformation().withScaleSettings(scaleSettings))
                    ),
                Duration.ofMinutes(2),
                () -> "Updating autoscale settings for compute cluster '%s' did not complete within 2 minutes".formatted(rComputeName)
            );
        } catch (ManagementException e) {
            // A 202 surfaced as an exception here is exactly the spurious case described above — the update was
            // actually accepted, so proceed to polling instead of failing. Any other status (400/404/409...) is a
            // genuine rejection and must still propagate.
            if (e.getResponse() == null || e.getResponse().getStatusCode() != 202) {
                throw translateScaleError(e, rComputeName);
            }
        }

        awaitProvisioningSucceeded(runContext, manager, rResourceGroupName, rWorkspaceName, rComputeName);

        logger.info("Updated autoscale settings of compute cluster '{}': min={}, max={}", rComputeName, rMinNodeCount, rMaxNodeCount);

        return Output.builder()
            .computeName(rComputeName)
            .minNodeCount(rMinNodeCount)
            .maxNodeCount(rMaxNodeCount)
            .build();
    }

    /**
     * Polls the compute cluster's own GET endpoint (the same {@code manager.computes().get(...)} call used to
     * fetch it above) until its {@link AmlCompute#provisioningState()} reaches a terminal value, mirroring
     * {@link AbstractComputeInstanceLifecycle}'s poll loop — the SDK's own LRO poller cannot be trusted to do this
     * for this specific operation, see the comment above this method's call site.
     */
    private static void awaitProvisioningSucceeded(RunContext runContext, MachineLearningManager manager, String resourceGroupName, String workspaceName, String computeName) {
        var logger = runContext.logger();
        AtomicReference<ProvisioningState> lastState = new AtomicReference<>();
        try {
            Await.until(
                () ->
                {
                    ComputeResource refreshed;
                    try {
                        refreshed = manager.computes().get(resourceGroupName, workspaceName, computeName);
                    } catch (ManagementException e) {
                        // A single transient ARM error must not fail a wait that spans up to 2 minutes — log and
                        // keep polling; a persistent problem still surfaces via the timeout below.
                        logger.warn("Transient error polling compute cluster '{}' provisioning state, will retry: {}", computeName, e.getMessage());
                        return false;
                    }
                    ProvisioningState state = refreshed.properties() instanceof AmlCompute amlCompute ? amlCompute.provisioningState() : null;
                    lastState.set(state);
                    if (state == ProvisioningState.FAILED || state == ProvisioningState.CANCELED) {
                        // A definitive failure is already known — don't burn the rest of the timeout waiting for a
                        // state that will never arrive.
                        throw new IllegalStateException("Updating autoscale settings for compute cluster '%s' failed (provisioning state '%s')".formatted(computeName, state));
                    }
                    return state == ProvisioningState.SUCCEEDED;
                },
                Duration.ofSeconds(5),
                Duration.ofMinutes(2)
            );
        } catch (TimeoutException e) {
            throw new IllegalStateException(
                "Updating autoscale settings for compute cluster '%s' did not complete within 2 minutes; last observed provisioning state was '%s'".formatted(computeName, lastState.get())
            );
        }
    }

    private static IllegalStateException translateScaleError(ManagementException e, String computeName) {
        if (e.getResponse() != null && e.getResponse().getStatusCode() == 409) {
            return new IllegalStateException(
                "Could not update compute cluster '%s' — an update is likely already in progress; wait for it to settle and retry".formatted(computeName), e
            );
        }
        return new IllegalStateException(
            "Failed to update autoscale settings for compute cluster '%s': %s".formatted(computeName, e.getValue() != null ? e.getValue().getMessage() : e.getMessage()), e
        );
    }

    @SuperBuilder
    @Getter
    @NoArgsConstructor
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Compute cluster name")
        private String computeName;

        @Schema(title = "Minimum node count")
        private Integer minNodeCount;

        @Schema(title = "Maximum node count")
        private Integer maxNodeCount;
    }
}
