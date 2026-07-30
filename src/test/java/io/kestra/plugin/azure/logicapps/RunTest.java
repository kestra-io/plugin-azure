package io.kestra.plugin.azure.logicapps;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import com.azure.core.credential.TokenCredential;
import com.azure.core.http.rest.Response;
import com.azure.core.management.profile.AzureProfile;
import com.azure.core.util.Context;
import com.azure.resourcemanager.logic.LogicManager;
import com.azure.resourcemanager.logic.models.WorkflowTriggers;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@KestraTest
class RunTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldTriggerWorkflow() throws Exception {
        WorkflowTriggers triggers = mock(WorkflowTriggers.class);
        @SuppressWarnings("unchecked")
        Response<Void> response = mock(Response.class);
        when(response.getStatusCode()).thenReturn(202);
        when(triggers.runWithResponse("rg", "workflow", "manual", Context.NONE)).thenReturn(response);

        LogicManager manager = LogicAppsTestHelper.managerWithTriggers(triggers);

        Run task = baseBuilder(Run.builder())
            .triggerName(Property.ofValue("manual"))
            .build();

        try (MockedStatic<LogicManager> mockedStatic = mockStatic(LogicManager.class)) {
            mockedStatic.when(() -> LogicManager.authenticate(any(TokenCredential.class), any(AzureProfile.class))).thenReturn(manager);

            Run.Output output = task.run(runContextFactory.of());

            assertThat(output.getWorkflowName(), is("workflow"));
            assertThat(output.getTriggerName(), is("manual"));
            assertThat(output.getStatusCode(), is(202));
            verify(triggers).runWithResponse("rg", "workflow", "manual", Context.NONE);
        }
    }

    private Run.RunBuilder<?, ?> baseBuilder(Run.RunBuilder<?, ?> builder) {
        return builder
            .id("run")
            .type(Run.class.getName())
            .tenantId(Property.ofValue("tenant"))
            .clientId(Property.ofValue("client"))
            .clientSecret(Property.ofValue("secret"))
            .subscriptionId(Property.ofValue("subscription"))
            .resourceGroupName(Property.ofValue("rg"))
            .workflowName(Property.ofValue("workflow"));
    }
}
