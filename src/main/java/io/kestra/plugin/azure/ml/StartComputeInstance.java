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
                id: azure_ml_start_compute_instance
                namespace: company.team

                tasks:
                  - id: start
                    type: io.kestra.plugin.azure.ml.StartComputeInstance
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
    title = "Start an Azure Machine Learning compute instance",
    description = "Starts a stopped compute instance and, by default, waits until it reports `RUNNING`. A no-op, without error, when the instance is already running."
)
public class StartComputeInstance extends AbstractComputeInstanceLifecycle {
    @Override
    protected String actionVerb() {
        return "start";
    }

    @Override
    protected ComputeInstanceState targetState() {
        return ComputeInstanceState.RUNNING;
    }

    @Override
    protected void invokeAction(MachineLearningManager manager, String resourceGroupName, String workspaceName, String computeName) {
        manager.computes().start(resourceGroupName, workspaceName, computeName);
    }
}
