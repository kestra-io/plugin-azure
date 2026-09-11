package io.kestra.plugin.azure.ml;

import com.azure.resourcemanager.machinelearning.MachineLearningManager;
import com.azure.resourcemanager.machinelearning.models.ComputeInstanceState;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;

import io.swagger.v3.oas.annotations.media.Schema;
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
                id: azure_ml_stop_compute_instance
                namespace: company.team

                tasks:
                  - id: stop
                    type: io.kestra.plugin.azure.ml.StopComputeInstance
                    tenantId: "{{ secret('AZURE_TENANT_ID') }}"
                    clientId: "{{ secret('AZURE_CLIENT_ID') }}"
                    clientSecret: "{{ secret('AZURE_CLIENT_SECRET') }}"
                    subscriptionId: "{{ secret('AZURE_SUBSCRIPTION_ID') }}"
                    resourceGroupName: ml-rg
                    workspaceName: ml-workspace
                    computeName: dev-instance
                """
        )
    }
)
@Schema(
    title = "Stop an Azure Machine Learning compute instance",
    description = "Stops a running compute instance and, by default, waits until it reports `STOPPED` — useful at the end of a flow to avoid paying for an idle instance. A no-op, without error, when the instance is already stopped."
)
public class StopComputeInstance extends AbstractComputeInstanceLifecycle {
    @Override
    protected String actionVerb() {
        return "stop";
    }

    @Override
    protected ComputeInstanceState targetState() {
        return ComputeInstanceState.STOPPED;
    }

    @Override
    protected void invokeAction(MachineLearningManager manager, String resourceGroupName, String workspaceName, String computeName) {
        manager.computes().stop(resourceGroupName, workspaceName, computeName);
    }
}
