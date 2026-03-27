package io.kestra.plugin.azure.streamanalytics;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import com.azure.core.credential.TokenCredential;
import com.azure.core.management.profile.AzureProfile;
import com.azure.resourcemanager.streamanalytics.StreamAnalyticsManager;
import com.azure.resourcemanager.streamanalytics.models.StreamingJob;
import com.azure.resourcemanager.streamanalytics.models.StreamingJobs;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@KestraTest
class GetJobTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldReturnJobDetails() throws Exception {
        // Setup mock objects
        StreamAnalyticsManager mockManager = mock(StreamAnalyticsManager.class);
        StreamingJobs mockStreamingJobs = mock(StreamingJobs.class);
        StreamingJob mockJob = mock(StreamingJob.class);

        when(mockManager.streamingJobs()).thenReturn(mockStreamingJobs);
        when(mockStreamingJobs.getByResourceGroup("test-rg", "test-job")).thenReturn(mockJob);
        when(mockJob.name()).thenReturn("test-job");
        when(mockJob.regionName()).thenReturn("eastus");
        when(mockJob.provisioningState()).thenReturn("Succeeded");
        when(mockJob.jobState()).thenReturn("Running");

        GetJob task = GetJob.builder()
            .id(GetJobTest.class.getSimpleName())
            .type(GetJob.class.getName())
            .subscriptionId(Property.ofValue("test-subscription-id"))
            .resourceGroup(Property.ofValue("test-rg"))
            .jobName(Property.ofValue("test-job"))
            .tenantId(Property.ofValue("test-tenant-id"))
            .clientId(Property.ofValue("test-client-id"))
            .clientSecret(Property.ofValue("test-client-secret"))
            .build();

        RunContext runContext = runContextFactory.of();

        // Mock static StreamAnalyticsManager.authenticate
        try (MockedStatic<StreamAnalyticsManager> mockedStatic = mockStatic(StreamAnalyticsManager.class)) {
            mockedStatic.when(() -> StreamAnalyticsManager.authenticate(any(TokenCredential.class), any(AzureProfile.class)))
                .thenReturn(mockManager);

            GetJob.Output output = task.run(runContext);

            assertThat(output.getJobName(), is("test-job"));
            assertThat(output.getLocation(), is("eastus"));
            assertThat(output.getProvisioningState(), is("Succeeded"));
            assertThat(output.getJobState(), is("Running"));
        }
    }

    @Test
    void shouldMapOutputCorrectly() throws Exception {
        // Setup mock objects
        StreamAnalyticsManager mockManager = mock(StreamAnalyticsManager.class);
        StreamingJobs mockStreamingJobs = mock(StreamingJobs.class);
        StreamingJob mockJob = mock(StreamingJob.class);

        when(mockManager.streamingJobs()).thenReturn(mockStreamingJobs);
        when(mockStreamingJobs.getByResourceGroup("prod-rg", "analytics-job")).thenReturn(mockJob);
        when(mockJob.name()).thenReturn("analytics-job");
        when(mockJob.regionName()).thenReturn("westeurope");
        when(mockJob.provisioningState()).thenReturn("Creating");
        when(mockJob.jobState()).thenReturn("Starting");

        GetJob task = GetJob.builder()
            .id(GetJobTest.class.getSimpleName())
            .type(GetJob.class.getName())
            .subscriptionId(Property.ofValue("sub-123"))
            .resourceGroup(Property.ofValue("prod-rg"))
            .jobName(Property.ofValue("analytics-job"))
            .tenantId(Property.ofValue("tenant-456"))
            .clientId(Property.ofValue("client-789"))
            .clientSecret(Property.ofValue("secret-abc"))
            .build();

        RunContext runContext = runContextFactory.of();

        try (MockedStatic<StreamAnalyticsManager> mockedStatic = mockStatic(StreamAnalyticsManager.class)) {
            mockedStatic.when(() -> StreamAnalyticsManager.authenticate(any(TokenCredential.class), any(AzureProfile.class)))
                .thenReturn(mockManager);

            GetJob.Output output = task.run(runContext);

            assertThat(output.getJobName(), is("analytics-job"));
            assertThat(output.getLocation(), is("westeurope"));
            assertThat(output.getProvisioningState(), is("Creating"));
            assertThat(output.getJobState(), is("Starting"));
        }
    }

    @Test
    void shouldThrowOnAzureFailure() throws Exception {
        StreamAnalyticsManager mockManager = mock(StreamAnalyticsManager.class);
        StreamingJobs mockStreamingJobs = mock(StreamingJobs.class);

        when(mockManager.streamingJobs()).thenReturn(mockStreamingJobs);
        when(mockStreamingJobs.getByResourceGroup(any(), any()))
            .thenThrow(new RuntimeException("Job not found"));

        GetJob task = GetJob.builder()
            .id(GetJobTest.class.getSimpleName())
            .type(GetJob.class.getName())
            .subscriptionId(Property.ofValue("sub-123"))
            .resourceGroup(Property.ofValue("missing-rg"))
            .jobName(Property.ofValue("missing-job"))
            .tenantId(Property.ofValue("tenant-456"))
            .clientId(Property.ofValue("client-789"))
            .clientSecret(Property.ofValue("secret-abc"))
            .build();

        RunContext runContext = runContextFactory.of();

        try (MockedStatic<StreamAnalyticsManager> mockedStatic = mockStatic(StreamAnalyticsManager.class)) {
            mockedStatic.when(() -> StreamAnalyticsManager.authenticate(any(TokenCredential.class), any(AzureProfile.class)))
                .thenReturn(mockManager);

            Exception exception = assertThrows(Exception.class, () -> task.run(runContext));
            assertThat(exception.getMessage(), containsString("Failed to retrieve Stream Analytics job"));
            assertThat(exception.getMessage(), containsString("missing-job"));
            assertThat(exception.getMessage(), containsString("missing-rg"));
            assertThat(exception.getCause(), is(notNullValue()));
        }
    }
}
