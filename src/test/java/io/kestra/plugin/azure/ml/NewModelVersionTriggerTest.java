package io.kestra.plugin.azure.ml;

import java.time.ZonedDateTime;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
@Disabled("Requires an Azure Machine Learning workspace with a registered model")
class NewModelVersionTriggerTest extends AbstractMachineLearningTaskTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void evaluateFiresOnFirstSeenVersion() throws Exception {
        NewModelVersionTrigger trigger = NewModelVersionTrigger.builder()
            .id(NewModelVersionTriggerTest.class.getSimpleName())
            .type(NewModelVersionTrigger.class.getName())
            .tenantId(TENANT_ID)
            .clientId(CLIENT_ID)
            .clientSecret(CLIENT_SECRET)
            .subscriptionId(SUBSCRIPTION_ID)
            .resourceGroupName(RESOURCE_GROUP_NAME)
            .workspaceName(WORKSPACE_NAME)
            .modelName(Property.ofExpression("{{ globals.azure.ml.modelName }}"))
            .build();

        RunContext runContext = runContextFactory.of();
        TriggerContext triggerContext = TriggerContext.builder()
            .namespace("company.team")
            .flowId("new_model_version_flow")
            .triggerId(trigger.getId())
            .date(ZonedDateTime.now())
            .build();

        var execution = trigger.evaluate(ConditionContext.builder().runContext(runContext).build(), triggerContext);

        assertThat(execution.isPresent(), is(true));
        execution.ifPresent(e -> assertThat(e.getState().getCurrent(), is(State.Type.CREATED)));
    }
}
