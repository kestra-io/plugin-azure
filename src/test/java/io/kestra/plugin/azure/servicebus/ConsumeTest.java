package io.kestra.plugin.azure.servicebus;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.utils.IdUtils;
import org.assertj.core.api.AbstractThrowableAssert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class ConsumeTest extends BaseServiceBusTest {
    @Test
    @ResourceLock("service-bus-comsumer-lock")
    void shouldConsumePublishedTextMessage() throws Exception {
        //region GIVEN
        String subscriptionName = publishToTopic(Message.builder()
            .body("example_message_body")
            .timeToLive(Duration.ofSeconds(10))
        );
        Consume consume = Consume.builder()
            .topicName(Property.ofValue(topicName))
            .connectionString(Property.ofValue(connectionString))
            .subscriptionName(Property.ofValue(subscriptionName))
            .maxReceiveDuration(Property.ofValue(Duration.ofSeconds(30)))
            .maxMessages(Property.ofValue(1))
            .build();
        //endregion

        //region WHEN
        Consume.Output output = consume.run(runContextFactory.of());
        //endregion

        //region WHEN
        assertThat(output.getCount()).isEqualTo(1);
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
        Consume consume = Consume.builder()
            .topicName(Property.ofValue(topicName))
            .connectionString(Property.ofValue(connectionString))
            .subscriptionName(Property.ofValue(subscriptionName))
            .serdeType(Property.ofValue(SerdeType.JSON))
            .maxReceiveDuration(Property.ofValue(Duration.ofSeconds(30)))
            .maxMessages(Property.ofValue(1))
            .build();
        //endregion

        //region WHEN
        Consume.Output output = consume.run(runContextFactory.of());
        //endregion

        //region WHEN
        assertThat(output.getCount()).isEqualTo(1);
        //endregion
    }

    @Test
    @ResourceLock("service-bus-comsumer-lock")
    void shouldReturnEmptyOutputWhenNoMessagesConsumed() throws Exception {
        //region GIVEN
        String subscriptionName = this.createSubscription(topicName, IdUtils.create());
        Consume consume = Consume.builder()
            .topicName(Property.ofValue(topicName))
            .connectionString(Property.ofValue(connectionString))
            .subscriptionName(Property.ofValue(subscriptionName))
            .maxReceiveDuration(Property.ofValue(Duration.ofSeconds(1)))
            .serdeType(Property.ofValue(SerdeType.JSON))
            .build();
        //endregion

        //region WHEN
        Consume.Output output = consume.run(runContextFactory.of());
        //endregion

        //region WHEN
        assertThat(output.getCount()).isEqualTo(0);
        //endregion
    }

    @Test
    @ResourceLock("service-bus-comsumer-lock")
    void shouldReturnEmptyOutputWhenNoMessagesConsumedAfterMaxDuration() throws Exception {
        //region GIVEN
        String subscriptionName = this.createSubscription(topicName, IdUtils.create());

        Consume consume = Consume.builder()
            .connectionString(Property.ofValue(connectionString))
            .topicName(Property.ofValue(topicName))
            .subscriptionName(Property.ofValue(subscriptionName))
            .subscriptionName(Property.ofValue(subscriptionName))
            .serdeType(Property.ofValue(SerdeType.JSON))
            .maxReceiveDuration(Property.ofValue(Duration.ofSeconds(1)))
            .build();
        //endregion

        //region WHEN
        Instant start = Instant.now();
        Consume.Output output = consume.run(runContextFactory.of());
        //endregion

        //region WHEN
        assertThat(output.getCount()).isEqualTo(0);
        assertThat(Instant.now().toEpochMilli() - start.toEpochMilli()).isGreaterThanOrEqualTo(1000);
        //endregion
    }

    @Test
    @ResourceLock("service-bus-comsumer-lock")
    void shouldReturnOutputWhenMaxMessagesReached() throws Exception {
        //region GIVEN
        String subscriptionName = this.createSubscription(topicName, IdUtils.create());
        publishToTopic(Message.builder()
            .body("{\"message\":\"example_message_body\"}")
            .timeToLive(Duration.ofSeconds(10)),
            subscriptionName
        );
        publishToTopic(Message.builder()
            .body("{\"message\":\"example_message_body\"}")
            .timeToLive(Duration.ofSeconds(10)),
            subscriptionName
        );
        Consume consume = Consume.builder()
            .topicName(Property.ofValue(topicName))
            .connectionString(Property.ofValue(connectionString))
            .subscriptionName(Property.ofValue(subscriptionName))
            .serdeType(Property.ofValue(SerdeType.JSON))
            .maxMessages(Property.ofValue(1))
            .maxReceiveDuration(Property.ofValue(Duration.ofSeconds(30)))
            .build();
        //endregion

        //region WHEN
        Consume.Output output = consume.run(runContextFactory.of());
        //endregion

        //region WHEN
        assertThat(output.getCount()).isEqualTo(1);
        //endregion
    }

    @Test
    void shouldThrowErrorWhenTopicAndQueueAreBothProvided() {
        //region GIVEN
        Consume consume = Consume.builder()
            .topicName(Property.ofValue(topicName))
            .queueName(Property.ofValue(queueName))
            .connectionString(Property.ofValue(connectionString))
            .maxReceiveDuration(Property.ofValue(Duration.ofSeconds(1)))
            .build();
        //endregion

        //region WHEN
        AbstractThrowableAssert<?, ?> throwableAssert = assertThatThrownBy(() -> consume.run(
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
        Consume consume = Consume.builder()
            .connectionString(Property.ofValue(connectionString))
            .maxReceiveDuration(Property.ofValue(Duration.ofSeconds(1)))
            .build();
        //endregion

        //region WHEN
        AbstractThrowableAssert<?, ?> throwableAssert = assertThatThrownBy(() -> consume.run(
            runContextFactory.of())
        );
        //endregion

        //region WHEN
        throwableAssert.isInstanceOf(IllegalVariableEvaluationException.class);
        throwableAssert.hasMessageContaining("Exactly one of queueName or topicName must be specified.");
        //endregion
    }
}