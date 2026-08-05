package io.kestra.plugin.azure.logicapps;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import com.azure.core.credential.TokenCredential;
import com.azure.core.management.profile.AzureProfile;
import com.azure.resourcemanager.logic.LogicManager;
import com.azure.resourcemanager.logic.models.Workflows;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@KestraTest
class GetTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldReturnWorkflowMetadata() throws Exception {
        Workflows workflows = mock(Workflows.class);
        var workflow = LogicAppsTestHelper.workflow("id-1", "workflow");
        when(workflows.getByResourceGroup("rg", "workflow")).thenReturn(workflow);
        LogicManager manager = LogicAppsTestHelper.managerWithWorkflows(workflows);

        Get task = Get.builder()
            .id("get")
            .type(Get.class.getName())
            .tenantId(Property.ofValue("tenant"))
            .clientId(Property.ofValue("client"))
            .clientSecret(Property.ofValue("secret"))
            .subscriptionId(Property.ofValue("subscription"))
            .resourceGroupName(Property.ofValue("rg"))
            .workflowName(Property.ofValue("workflow"))
            .build();

        try (MockedStatic<LogicManager> mockedStatic = mockStatic(LogicManager.class)) {
            mockedStatic.when(() -> LogicManager.authenticate(any(TokenCredential.class), any(AzureProfile.class))).thenReturn(manager);

            Get.Output output = task.run(runContextFactory.of());

            assertThat(output.getWorkflow().getName(), is("workflow"));
            assertThat(output.getWorkflow().getState(), is("Enabled"));
            assertThat(output.getWorkflow().getProvisioningState(), is("Succeeded"));
        }
    }
}
