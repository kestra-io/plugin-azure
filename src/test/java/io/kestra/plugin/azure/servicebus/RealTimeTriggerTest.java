package io.kestra.plugin.azure.servicebus;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.Trigger;
import io.kestra.core.utils.TestsUtils;
import org.assertj.core.api.AbstractThrowableAssert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.ResourceLock;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;


class RealTimeTriggerTest extends BaseServiceBusTest{

    @Test
    @Timeout(10)
    @ResourceLock("service-bus-comsumer-lock")
    void shouldConsumePublishedTextMessage() throws Exception {
        //region GIVEN
        String messageBody = "example_message_body";
        publishToTopic(Message.builder()
            .body(messageBody)
            .timeToLive(Duration.ofSeconds(10))
        );
        RealTimeTrigger realTimeTrigger = RealTimeTrigger.builder()
            .id(RealTimeTriggerTest.class.getSimpleName())
            .type(RealTimeTrigger.class.getSimpleName())
            .connectionString(Property.ofValue(connectionString))
            .topicName(Property.ofValue(topicName))
            .connectionString(Property.ofValue(connectionString))
            .subscriptionName(Property.ofValue(subscriptionName))
            .build();

        Map.Entry<ConditionContext, Trigger> context =TestsUtils.mockTrigger(runContextFactory, realTimeTrigger);


        //endregion

        //region WHEN
        Flux<Execution> output = realTimeTrigger.evaluate(
            context.getKey(),
            context.getValue()
        );
        //endregion

        //region WHEN

        Execution execution = output.blockFirst();

        assertThat(execution).isNotNull();
        assertThat(execution.getTrigger()).isNotNull();
        assertThat(execution.getTrigger().getVariables().get("body")).isEqualTo(messageBody);
        //endregion
    }

    @Test
    @Timeout(10)
    @ResourceLock("service-bus-comsumer-lock")
    void shouldConsumePublishedJsonMessage() throws Exception {
        //region GIVEN
        String messageBody = "{\"message\":\"example_message_body\"}";
        publishToTopic(Message.builder()
            .body(messageBody)
            .timeToLive(Duration.ofSeconds(10))
        );
        RealTimeTrigger realTimeTrigger = RealTimeTrigger.builder()
            .id(RealTimeTriggerTest.class.getSimpleName())
            .type(RealTimeTrigger.class.getSimpleName())
            .connectionString(Property.ofValue(connectionString))
            .topicName(Property.ofValue(topicName))
            .connectionString(Property.ofValue(connectionString))
            .subscriptionName(Property.ofValue(subscriptionName))
            .build();

        Map.Entry<ConditionContext, Trigger> context =TestsUtils.mockTrigger(runContextFactory, realTimeTrigger);


        //endregion

        //region WHEN
        Flux<Execution> output = realTimeTrigger.evaluate(
            context.getKey(),
            context.getValue()
        );
        //endregion

        //region WHEN

        Execution execution = output.blockFirst();

        assertThat(execution).isNotNull();
        assertThat(execution.getTrigger()).isNotNull();
        assertThat(execution.getTrigger().getVariables().get("body")).isEqualTo(messageBody);
        //endregion
    }


    @Test
    void shouldThrowErrorWhenTopicAndQueueAreBothProvided() {
        //region GIVEN
        RealTimeTrigger realTimeTrigger = RealTimeTrigger.builder()
            .id(RealTimeTriggerTest.class.getSimpleName())
            .type(RealTimeTrigger.class.getSimpleName())
            .connectionString(Property.ofValue(connectionString))
            .topicName(Property.ofValue(topicName))
            .queueName(Property.ofValue(queueName))
            .connectionString(Property.ofValue(connectionString))
            .subscriptionName(Property.ofValue(subscriptionName))
            .build();

        Map.Entry<ConditionContext, Trigger> context =TestsUtils.mockTrigger(runContextFactory, realTimeTrigger);


        //endregion

        //region WHEN
        AbstractThrowableAssert<?, ?> throwableAssert = assertThatThrownBy(() -> realTimeTrigger.evaluate(
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
        RealTimeTrigger realTimeTrigger = RealTimeTrigger.builder()
            .id(RealTimeTriggerTest.class.getSimpleName())
            .type(RealTimeTrigger.class.getSimpleName())
            .connectionString(Property.ofValue(connectionString))
            .connectionString(Property.ofValue(connectionString))
            .subscriptionName(Property.ofValue(subscriptionName))
            .build();

        Map.Entry<ConditionContext, Trigger> context =TestsUtils.mockTrigger(runContextFactory, realTimeTrigger);
        //endregion

        //region WHEN
        AbstractThrowableAssert<?, ?> throwableAssert = assertThatThrownBy(() -> realTimeTrigger.evaluate(
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