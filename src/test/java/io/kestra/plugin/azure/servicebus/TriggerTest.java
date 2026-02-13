package io.kestra.plugin.azure.servicebus;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import org.assertj.core.api.AbstractThrowableAssert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@EnabledIfEnvironmentVariable(named = "AZURE_CONNECTION_STRING", matches = ".+")
class TriggerTest extends BaseServiceBusTest {
    @Test
    @ResourceLock("service-bus-comsumer-lock")
    void shouldConsumePublishedTextMessage() throws Exception {
        //region GIVEN
        String subscriptionName = publishToTopic(Message.builder()
            .body("example_message_body")
            .timeToLive(Duration.ofSeconds(10))
        );
        Trigger trigger = Trigger.builder()
            .id(TriggerTest.class.getSimpleName())
            .type(TriggerTest.class.getSimpleName())
            .topicName(Property.ofValue(topicName))
            .connectionString(Property.ofValue(connectionString))
            .subscriptionName(Property.ofValue(subscriptionName))
            .maxReceiveDuration(Property.ofValue(Duration.ofSeconds(30)))
            .maxMessages(Property.ofValue(1))
            .build();

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = TestsUtils.mockTrigger(runContextFactory, trigger);

        //endregion

        //region WHEN
        Optional<Execution> execution = trigger.evaluate(
            context.getKey(),
            context.getValue()
        );
        //endregion

        //region WHEN
        assertThat(execution.isPresent()).isTrue();
        assertThat(execution.get().getTrigger()).isNotNull();
        assertThat(execution.get().getTrigger().getVariables().get("count")).isEqualTo(1);
        //endregion
    }

    @Test
    @ResourceLock("service-bus-comsumer-lock")
    void shouldConsumePublishedJsonMessage() throws Exception {
        //region GIVEN
        String subscriptionName = publishToTopic(Message.builder()
            .body("{\"message\":\"example_message_body\"}")
            .timeToLive(Duration.ofSeconds(10))
        );

        Trigger trigger = Trigger.builder()
            .id(TriggerTest.class.getSimpleName())
            .type(TriggerTest.class.getSimpleName())
            .topicName(Property.ofValue(topicName))
            .connectionString(Property.ofValue(connectionString))
            .subscriptionName(Property.ofValue(subscriptionName))
            .maxReceiveDuration(Property.ofValue(Duration.ofSeconds(30)))
            .maxMessages(Property.ofValue(1))
            .build();

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = TestsUtils.mockTrigger(runContextFactory, trigger);
        //endregion

        //region WHEN
        Optional<Execution> execution = trigger.evaluate(
            context.getKey(),
            context.getValue()
        );
        //endregion

        //region WHEN
        assertThat(execution.isPresent()).isTrue();
        assertThat(execution.get().getTrigger()).isNotNull();
        assertThat(execution.get().getTrigger().getVariables().get("count")).isEqualTo(1);
        //endregion
    }

    @Test
    @ResourceLock("service-bus-comsumer-lock")
    void shouldReturnEmptyOptionalWhenNoMessagesAreConsumed() throws Exception {
        //region GIVEN
        String subscriptionName = createSubscription(topicName, IdUtils.create());

        Trigger trigger = Trigger.builder()
            .id(TriggerTest.class.getSimpleName())
            .type(TriggerTest.class.getSimpleName())
            .topicName(Property.ofValue(topicName))
            .connectionString(Property.ofValue(connectionString))
            .subscriptionName(Property.ofValue(subscriptionName))
            .maxReceiveDuration(Property.ofValue(Duration.ofSeconds(1)))
            .build();

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = TestsUtils.mockTrigger(runContextFactory, trigger);
        //endregion

        //region WHEN
        Optional<Execution> execution = trigger.evaluate(
            context.getKey(),
            context.getValue()
        );
        //endregion

        //region WHEN
        assertThat(execution.isEmpty()).isTrue();
        //endregion
    }

    @Test
    void shouldThrowErrorWhenTopicAndQueueAreBothProvided() {
        //region GIVEN
        Trigger trigger = Trigger.builder()
            .id(TriggerTest.class.getSimpleName())
            .type(TriggerTest.class.getSimpleName())
            .topicName(Property.ofValue(topicName))
            .queueName(Property.ofValue(queueName))
            .connectionString(Property.ofValue(connectionString))
            .maxReceiveDuration(Property.ofValue(Duration.ofSeconds(1)))
            .build();

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = TestsUtils.mockTrigger(runContextFactory, trigger);
        //endregion

        //region WHEN

        AbstractThrowableAssert<?, ?> throwableAssert = assertThatThrownBy(() -> trigger.evaluate(
                context.getKey(),
                context.getValue()
            )
        );
        //endregion

        //region WHEN
        throwableAssert.isInstanceOf(IllegalVariableEvaluationException.class);
        throwableAssert.hasMessageContaining("Exactly one of queueName or topicName must be specified.");
        //endregion
    }

    @Test
    void shouldThrowErrorWhenNeitherTopicAndQueueAreProvided() {
        //region GIVEN
        Trigger trigger = Trigger.builder()
            .id(TriggerTest.class.getSimpleName())
            .type(TriggerTest.class.getSimpleName())
            .connectionString(Property.ofValue(connectionString))
            .maxReceiveDuration(Property.ofValue(Duration.ofSeconds(1)))
            .build();

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = TestsUtils.mockTrigger(runContextFactory, trigger);
        //endregion

        //region WHEN
        AbstractThrowableAssert<?, ?> throwableAssert = assertThatThrownBy(() -> trigger.evaluate(
                context.getKey(),
                context.getValue()
            )
        );
        //endregion

        //region WHEN
        throwableAssert.isInstanceOf(IllegalVariableEvaluationException.class);
        throwableAssert.hasMessageContaining("Exactly one of queueName or topicName must be specified.");
        //endregion
    }
}