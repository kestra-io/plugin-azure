package io.kestra.plugin.azure.ml;

import com.azure.core.management.exception.ManagementException;
import com.azure.resourcemanager.machinelearning.MachineLearningManager;
import com.azure.resourcemanager.machinelearning.models.ComputeInstance;
import com.azure.resourcemanager.machinelearning.models.ComputeInstanceState;
import com.azure.resourcemanager.machinelearning.models.ComputeResource;

/**
 * Pre-checks shared by every task that submits work to a compute target, so a missing or stopped compute surfaces
 * an actionable message instead of an opaque ARM 404/409.
 */
final class MachineLearningComputeService {
    private MachineLearningComputeService() {
    }

    static void ensureComputeUsable(MachineLearningManager manager, String resourceGroupName, String workspaceName, String computeName) {
        ComputeResource compute;
        try {
            compute = manager.computes().get(resourceGroupName, workspaceName, computeName);
        } catch (ManagementException e) {
            if (e.getResponse() != null && e.getResponse().getStatusCode() == 404) {
                throw new IllegalArgumentException(
                    "Compute target '%s' was not found in workspace '%s' — check `computeName`, `resourceGroupName` and `workspaceName`".formatted(computeName, workspaceName), e
                );
            }
            throw e;
        }

        if (compute.properties() instanceof ComputeInstance computeInstance) {
            ComputeInstanceState state = computeInstance.properties() != null ? computeInstance.properties().state() : null;
            if (state == ComputeInstanceState.STOPPED || state == ComputeInstanceState.STOPPING) {
                throw new IllegalStateException("Compute instance '%s' is %s — start it first with the StartComputeInstance task".formatted(computeName, state.toString().toLowerCase()));
            }
        }
    }
}
