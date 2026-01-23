package io.kestra.plugin.azure.servicebus;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import org.assertj.core.api.AbstractThrowableAssert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class PublishTest extends BaseServiceBusTest {
    @Test
    @ResourceLock("service-bus-comsumer-lock")
    void shouldPublishToTopic() throws Exception {
        //region GIVEN
        Publish publish = Publish.builder()
            .from(Property.ofValue(messages))
            .topicName(Property.ofValue(topicName))
            .connectionString(Property.ofValue(connectionString))
            .build();
        //endregion

        //region WHEN
        Publish.Output output = publish.run(runContextFactory.of());
        //endregion

        //region WHEN
        assertThat(output.messagesCount()).isEqualTo(1);
        //endregion
    }

    @Test
    @ResourceLock("service-bus-comsumer-lock")
    void shouldPublishToQueue() throws Exception {
        //region GIVEN
        Publish publish = Publish.builder()
            .from(Property.ofValue(messages))
            .queueName(Property.ofValue(queueName))
            .connectionString(Property.ofValue(connectionString))
            .build();
        //endregion

        //region WHEN
        Publish.Output output = publish.run(runContextFactory.of());
        //endregion

        //region WHEN
        assertThat(output.messagesCount()).isEqualTo(1);
        //endregion
    }

    @Test
    @ResourceLock("service-bus-comsumer-lock")
    void shouldPublishMultipleMessages() throws Exception {
        //region GIVEN
        Publish publish = Publish.builder()
            .from(Property.ofValue(List.of(singleMessage, singleMessage)))
            .topicName(Property.ofValue(topicName))
            .connectionString(Property.ofValue(connectionString))
            .build();
        //endregion

        //region WHEN
        Publish.Output output = publish.run(runContextFactory.of());
        //endregion

        //region WHEN
        assertThat(output.messagesCount()).isEqualTo(2);
        //endregion
    }

    @Test
    @ResourceLock("service-bus-comsumer-lock")
    void shouldPublishFromStringValue() throws Exception {
        //region GIVEN
        Publish publish = Publish.builder()
            .from(Property.ofValue("{\"body\":\"messageBody\"}"))
            .topicName(Property.ofValue(topicName))
            .connectionString(Property.ofValue(connectionString))
            .build();
        //endregion

        //region WHEN
        Publish.Output output = publish.run(runContextFactory.of());
        //endregion

        //region WHEN
        assertThat(output.messagesCount()).isEqualTo(1);
        //endregion
    }

    @Test
    void shouldThrowErrorWhenTopicAndQueueAreBothProvided() {
        //region GIVEN
        Publish publish = Publish.builder()
            .from(Property.ofValue(messages))
            .topicName(Property.ofValue(topicName))
            .queueName(Property.ofValue(queueName))
            .connectionString(Property.ofValue(connectionString))
            .build();
        //endregion

        //region WHEN
        AbstractThrowableAssert<?, ?> throwableAssert = assertThatThrownBy(() -> publish.run(
            runContextFactory.of())
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
        Publish publish = Publish.builder()
            .from(Property.ofValue(messages))
            .connectionString(Property.ofValue(connectionString))
            .build();
        //endregion

        //region WHEN
        AbstractThrowableAssert<?, ?> throwableAssert = assertThatThrownBy(() -> publish.run(
            runContextFactory.of())
        );
        //endregion

        //region WHEN
        throwableAssert.isInstanceOf(IllegalVariableEvaluationException.class);
        throwableAssert.hasMessageContaining("Exactly one of queueName or topicName must be specified.");
        //endregion
    }
}