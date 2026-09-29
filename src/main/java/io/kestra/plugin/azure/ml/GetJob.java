package io.kestra.plugin.azure.ml;

import java.net.URI;
import java.util.Map;

import com.azure.core.management.exception.ManagementException;
import com.azure.resourcemanager.machinelearning.MachineLearningManager;
import com.azure.resourcemanager.machinelearning.models.JobBase;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
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
                id: azure_ml_get_job
                namespace: company.team

                inputs:
                  - id: job_name
                    type: STRING

                tasks:
                  - id: get_job
                    type: io.kestra.plugin.azure.ml.GetJob
                    tenantId: "{{ secret('AZURE_TENANT_ID') }}"
                    clientId: "{{ secret('AZURE_CLIENT_ID') }}"
                    clientSecret: "{{ secret('AZURE_CLIENT_SECRET') }}"
                    subscriptionId: "{{ secret('AZURE_SUBSCRIPTION_ID') }}"
                    resourceGroupName: ml-rg
                    workspaceName: ml-workspace
                    jobName: "{{ inputs.job_name }}"
                """
        )
    }
)
@Schema(
    title = "Get the state, metrics and outputs of an Azure Machine Learning job",
    description = "Reads a job's current status and named outputs from the ARM control plane. Metrics are logged through MLflow rather than the ARM API, so they are fetched separately from the workspace's MLflow tracking server using the same Azure AD credentials; if that call fails (e.g. the service principal lacks the required scope), `metrics` is returned empty and a warning is logged instead of failing the task."
)
public class GetJob extends AbstractMachineLearningTask implements RunnableTask<GetJob.Output> {
    @Schema(title = "Job name", description = "Name of the Azure Machine Learning job to inspect")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> jobName;

    @Override
    public Output run(RunContext runContext) throws Exception {
        String rResourceGroupName = rResourceGroupName(runContext);
        String rWorkspaceName = rWorkspaceName(runContext);
        String rJobName = runContext.render(this.jobName).as(String.class).orElseThrow();

        MachineLearningManager manager = machineLearningManager(runContext);

        JobBase job;
        try {
            job = manager.jobs().get(rResourceGroupName, rWorkspaceName, rJobName);
        } catch (ManagementException e) {
            if (e.getResponse() != null && e.getResponse().getStatusCode() == 404) {
                throw new IllegalArgumentException(
                    "Job '%s' was not found in workspace '%s' — check `jobName`, `resourceGroupName` and `workspaceName`".formatted(rJobName, rWorkspaceName), e
                );
            }
            throw e;
        }

        JobState state = MachineLearningService.toJobState(job);

        Map<String, Double> metrics = MachineLearningService.mlflowMetrics(
            runContext,
            credentials(runContext),
            manager,
            rResourceGroupName,
            rWorkspaceName,
            rJobName
        );

        return Output.builder()
            .jobName(rJobName)
            .status(state)
            .studioUrl(studioUrl(runContext, rJobName))
            .metrics(metrics)
            .outputs(MachineLearningService.namedOutputs(MachineLearningService.jobOutputs(job)))
            .build();
    }

    @SuperBuilder
    @Getter
    @NoArgsConstructor
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Job name")
        private String jobName;

        @Schema(title = "Job status")
        private JobState status;

        @Schema(title = "Studio URL", description = "Deep link to the run in Azure ML Studio")
        private String studioUrl;

        @Schema(
            title = "Metrics",
            description = """
                Metrics logged by the run, keyed by metric name.

                Azure Machine Learning logs job metrics through MLflow, not through the ARM control-plane API used \
                for everything else in this task. This value is fetched best-effort by reading the workspace's MLflow \
                tracking URI and calling its REST API directly with the same Azure AD bearer token used to authenticate \
                this task. If that call fails (e.g. the service principal lacks the required scope, or the endpoint is \
                unreachable), a warning is logged and this field is an empty map — the task does not fail because of it.
                """
        )
        private Map<String, Double> metrics;

        @Schema(title = "Outputs", description = "Named job outputs, keyed by output name, pointing to their storage URI")
        private Map<String, URI> outputs;
    }
}
