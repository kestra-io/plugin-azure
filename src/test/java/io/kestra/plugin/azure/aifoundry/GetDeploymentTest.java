package io.kestra.plugin.azure.aifoundry;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import com.azure.ai.projects.AIProjectClientBuilder;
import com.azure.ai.projects.DeploymentsClient;
import com.azure.ai.projects.models.Deployment;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@KestraTest
class GetDeploymentTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run_withApiKey_throwsIllegalArgumentWithGuidance() throws Exception {
        GetDeployment task = GetDeployment.builder()
            .id("get-deployment")
            .type(GetDeployment.class.getName())
            .endpoint(Property.ofValue("https://test.api.azureml.ms/"))
            .apiKey(Property.ofValue("some-key"))
            .deploymentName(Property.ofValue("gpt-4o"))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> task.run(runContext)
        );
        assertThat(ex.getMessage(), containsString("Entra ID"));
    }

    @Test
    void run_withMockedClient_returnsDeploymentDetailsAndVerifiesArgs() throws Exception {
        GetDeployment task = GetDeployment.builder()
            .id("get-deployment")
            .type(GetDeployment.class.getName())
            .endpoint(Property.ofValue("https://test.api.azureml.ms/"))
            .deploymentName(Property.ofValue("gpt-4o"))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        Deployment mockDeployment = mock(Deployment.class);
        when(mockDeployment.getName()).thenReturn("gpt-4o");

        DeploymentsClient mockClient = mock(DeploymentsClient.class);
        when(mockClient.getDeployment(anyString())).thenReturn(mockDeployment);

        try (MockedConstruction<AIProjectClientBuilder> ignored = Mockito.mockConstruction(AIProjectClientBuilder.class, (mock, ctx) ->
        {
            when(mock.endpoint(anyString())).thenReturn(mock);
            when(mock.credential(any())).thenReturn(mock);
            when(mock.buildDeploymentsClient()).thenReturn(mockClient);
        })) {

            GetDeployment.Output output = task.run(runContext);

            assertThat(output, notNullValue());
            assertThat(output.getName(), is("gpt-4o"));

            verify(mockClient).getDeployment("gpt-4o");
        }
    }
}
