package io.kestra.plugin.azure.streamanalytics;

import org.slf4j.Logger;

import com.azure.core.management.AzureEnvironment;
import com.azure.core.management.profile.AzureProfile;
import com.azure.resourcemanager.streamanalytics.StreamAnalyticsManager;
import com.azure.resourcemanager.streamanalytics.models.StreamingJob;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.shared.AbstractAzureIdentityConnection;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
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
                id: azure_stream_analytics_get_job
                namespace: company.team

                tasks:
                  - id: get_job
                    type: io.kestra.plugin.azure.streamanalytics.GetJob
                    subscriptionId: "{{ secret('AZURE_SUBSCRIPTION_ID') }}"
                    resourceGroup: my-resource-group
                    jobName: my-stream-analytics-job
                    tenantId: "{{ secret('AZURE_TENANT_ID') }}"
                    clientId: "{{ secret('AZURE_CLIENT_ID') }}"
                    clientSecret: "{{ secret('AZURE_CLIENT_SECRET') }}"
                """
        )
    }
)
@Schema(
    title = "Get Azure Stream Analytics job details",
    description = "Fetch the current state and metadata of an Azure Stream Analytics job. " +
        "Use this task to poll job status in conditional flows, verify a job is running before triggering downstream tasks, " +
        "or surface provisioning errors. Authenticates using an Azure AD service principal."
)
public class GetJob extends AbstractAzureIdentityConnection implements RunnableTask<GetJob.Output> {

    @Schema(title = "Subscription ID", description = "Azure subscription GUID that owns the Stream Analytics job")
    @NotNull
    private Property<String> subscriptionId;

    @Schema(title = "Resource group name", description = "Resource group containing the Stream Analytics job")
    @NotNull
    private Property<String> resourceGroup;

    @Schema(title = "Job name", description = "Name of the Stream Analytics job to retrieve")
    @NotNull
    private Property<String> jobName;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        String rSubscriptionId = runContext.render(this.subscriptionId).as(String.class).orElseThrow();
        String rResourceGroup = runContext.render(this.resourceGroup).as(String.class).orElseThrow();
        String rJobName = runContext.render(this.jobName).as(String.class).orElseThrow();

        logger.info("Fetching Stream Analytics job '{}' in resource group '{}'", rJobName, rResourceGroup);

        try {
            StreamAnalyticsManager manager = streamAnalyticsManager(runContext, rSubscriptionId);

            StreamingJob job = manager.streamingJobs().getByResourceGroup(rResourceGroup, rJobName);

            logger.info(
                "Successfully retrieved Stream Analytics job '{}' with provisioning state '{}'",
                job.name(), job.provisioningState()
            );

            return Output.builder()
                .jobName(job.name())
                .location(job.regionName())
                .provisioningState(job.provisioningState())
                .jobState(job.jobState())
                .build();
        } catch (Exception e) {
            throw new Exception(
                "Failed to retrieve Stream Analytics job '" + rJobName +
                    "' in resource group '" + rResourceGroup + "'",
                e
            );
        }
    }

    private StreamAnalyticsManager streamAnalyticsManager(RunContext runContext, String subscriptionId)
        throws IllegalVariableEvaluationException {
        String tenantId = runContext.render(this.tenantId).as(String.class).orElse(null);
        AzureProfile profile = new AzureProfile(tenantId, subscriptionId, AzureEnvironment.AZURE);
        return StreamAnalyticsManager.authenticate(credentials(runContext), profile);
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Job name", description = "Name of the Stream Analytics job")
        private final String jobName;

        @Schema(title = "Location", description = "Azure region where the job is deployed")
        private final String location;

        @Schema(title = "Provisioning state", description = "Provisioning state of the job. " +
            "Possible values: Succeeded, Failed, Canceled, Provisioning, Deleting.")
        private final String provisioningState;

        @Schema(title = "Job state", description = "Runtime state of the job. " +
            "Possible values: Created, Starting, Running, Stopping, Stopped, Deleting, Failed, Degraded, Disabled, Scaling.")
        private final String jobState;
    }
}
