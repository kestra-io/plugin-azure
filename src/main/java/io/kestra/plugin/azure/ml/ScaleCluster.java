package io.kestra.plugin.azure.ml;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.azure.core.management.exception.ManagementException;
import com.azure.resourcemanager.machinelearning.MachineLearningManager;
import com.azure.resourcemanager.machinelearning.models.AmlCompute;
import com.azure.resourcemanager.machinelearning.models.ComputeResource;
import com.azure.resourcemanager.machinelearning.models.ScaleSettings;
import com.azure.resourcemanager.machinelearning.models.ScaleSettingsInformation;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;

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
    @Min(0)
    @Max(1000)
    @PluginProperty(group = "main")
    private Property<Integer> minNodeCount = Property.ofValue(0);

    @Schema(title = "Maximum node count", description = "Maximum number of nodes the cluster can scale out to")
    @NotNull
    @Min(1)
    @Max(1000)
    @PluginProperty(group = "main")
    private Property<Integer> maxNodeCount;

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

        // .apply() blocks with no client-side timeout of its own — bound it explicitly so a stalled ARM
        // long-running operation fails cleanly instead of hanging the task indefinitely.
        CompletableFuture<ComputeResource> future = CompletableFuture.supplyAsync(() ->
            compute.update()
                .withProperties(new ScaleSettingsInformation().withScaleSettings(scaleSettings))
                .apply()
        );
        try {
            future.get(2, TimeUnit.MINUTES);
        } catch (TimeoutException e) {
            // Best-effort: this attempts to interrupt the underlying blocking call rather than leaving it running
            // unobserved on a shared thread pool after this task has already reported failure.
            future.cancel(true);
            throw new IllegalStateException("Updating autoscale settings for compute cluster '%s' did not complete within 2 minutes".formatted(rComputeName), e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof ManagementException managementException) {
                throw translateScaleError(managementException, rComputeName);
            }
            throw new IllegalStateException("Failed to update autoscale settings for compute cluster '%s': %s".formatted(rComputeName, cause.getMessage()), cause);
        } catch (ManagementException e) {
            throw translateScaleError(e, rComputeName);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while updating autoscale settings for compute cluster '%s'".formatted(rComputeName), e);
        }

        logger.info("Updated autoscale settings of compute cluster '{}': min={}, max={}", rComputeName, rMinNodeCount, rMaxNodeCount);

        return Output.builder()
            .computeName(rComputeName)
            .minNodeCount(rMinNodeCount)
            .maxNodeCount(rMaxNodeCount)
            .build();
    }

    private static IllegalStateException translateScaleError(ManagementException e, String computeName) {
        if (e.getResponse() != null && e.getResponse().getStatusCode() == 409) {
            return new IllegalStateException(
                "Could not update compute cluster '%s' — an update is likely already in progress; wait for it to settle and retry".formatted(computeName), e
            );
        }
        return new IllegalStateException("Failed to update autoscale settings for compute cluster '%s': %s".formatted(computeName, e.getValue() != null ? e.getValue().getMessage() : e.getMessage()), e);
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
