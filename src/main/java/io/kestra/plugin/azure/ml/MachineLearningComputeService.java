package io.kestra.plugin.azure.ml;

import com.azure.core.management.exception.ManagementException;
import com.azure.resourcemanager.machinelearning.MachineLearningManager;
import com.azure.resourcemanager.machinelearning.models.ComputeInstance;
import com.azure.resourcemanager.machinelearning.models.ComputeInstanceState;
import com.azure.resourcemanager.machinelearning.models.ComputeResource;

/**
 * Pre-checks the named compute target so a missing or stopped compute surfaces an actionable message instead of an
 * opaque ARM 404/409. Called by {@link SubmitCommandJob}, which submits against a single named compute; there is no
 * equivalent single {@code computeName} on {@link SubmitPipelineJob} to check — its steps are a raw, user-supplied
 * pipeline graph where each step names its own compute independently.
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
            // Allow-list rather than a deny-list of known-bad states: any state other than one that means "up and
            // able to take work" is treated as not (yet) usable, so a state this list doesn't know about (a
            // provisioning/failure state added by Azure, or a genuinely unset state) still surfaces an actionable
            // error instead of silently falling through to submit against a compute that isn't really ready.
            boolean usable = state == ComputeInstanceState.RUNNING || state == ComputeInstanceState.JOB_RUNNING;
            if (!usable) {
                String stateLabel = state != null ? state.toString() : "in an unknown state";
                throw new IllegalStateException(
                    "Compute instance '%s' is not ready to accept work (%s) — if it is stopped, start it first with the StartComputeInstance task, otherwise wait for it to finish transitioning".formatted(computeName, stateLabel)
                );
            }
        }
    }
}
