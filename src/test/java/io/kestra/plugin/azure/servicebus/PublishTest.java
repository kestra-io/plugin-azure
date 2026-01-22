package io.kestra.plugin.azure.servicebus;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import org.assertj.core.api.AbstractThrowableAssert;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class PublishTest extends BaseServiceBusTest {
    private static final Map<String, Object> singleMessage = Map.of(
        "messageId", "messangeId",
        "body", "messageBodyExample"
    );
    private static final List<Map<String, Object>> messages = List.of(singleMessage);


    @Test
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
    void shouldPublishToQueue() throws Exception {
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
    void shouldThrowErrorWhenTopicAndQueueAreBothProvided() throws Exception {
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
    void shouldThrowErrorWhenNeitherTopicAndQueueAreProvided() throws Exception {
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