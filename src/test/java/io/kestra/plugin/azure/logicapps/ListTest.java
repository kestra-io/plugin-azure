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
class ListTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldListWorkflows() throws Exception {
        Workflows workflows = mock(Workflows.class);
        var workflowA = LogicAppsTestHelper.workflow("id-1", "workflow-a");
        var workflowB = LogicAppsTestHelper.workflow("id-2", "workflow-b");
        when(workflows.listByResourceGroup("rg"))
            .thenReturn(LogicAppsTestHelper.paged(workflowA, workflowB));
        LogicManager manager = LogicAppsTestHelper.managerWithWorkflows(workflows);

        List task = List.builder()
            .id("list")
            .type(List.class.getName())
            .tenantId(Property.ofValue("tenant"))
            .clientId(Property.ofValue("client"))
            .clientSecret(Property.ofValue("secret"))
            .subscriptionId(Property.ofValue("subscription"))
            .resourceGroupName(Property.ofValue("rg"))
            .maxWorkflows(Property.ofValue(1))
            .build();

        try (MockedStatic<LogicManager> mockedStatic = mockStatic(LogicManager.class)) {
            mockedStatic.when(() -> LogicManager.authenticate(any(TokenCredential.class), any(AzureProfile.class))).thenReturn(manager);

            List.Output output = task.run(runContextFactory.of());

            assertThat(output.getTotal(), is(1));
            assertThat(output.getWorkflows().getFirst().getName(), is("workflow-a"));
        }
    }
}
