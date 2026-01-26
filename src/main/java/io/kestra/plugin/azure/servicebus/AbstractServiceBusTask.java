package io.kestra.plugin.azure.servicebus;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.azure.AbstractAzureIdentityConnection;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.Optional;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractServiceBusTask extends AbstractAzureIdentityConnection {
    @Schema(description = QUEUE_NAME_DESCRIPTION)
    protected Property<String> queueName;

    @Schema(description = TOPIC_NAME_DESCRIPTION)
    protected Property<String> topicName;

    @Schema(description = CONNECTION_STRING_DESCRIPTION)
    protected Property<String> connectionString;

    @Schema(description = SUBSCRIPTION_NAME_DESCRIPTION)
    protected Property<String> subscriptionName;

    @Builder.Default
    @NotNull
    @Schema(title = SERDE_TYPE_DESCRIPTION)
    protected Property<SerdeType> serdeType = Property.ofValue(DEFAULT_SERDE_TYPE);

    protected static final SerdeType DEFAULT_SERDE_TYPE = SerdeType.STRING;

    protected static final ObjectMapper OBJECT_MAPPER = JacksonMapper.ofJson(false);

    public static final String QUEUE_NAME_DESCRIPTION = """
        The name of the Service Bus queue to publish messages to. \
        queueName and topicName must be set exclusively.
        """;

    public static final String TOPIC_NAME_DESCRIPTION = """
        The name of the Service Bus topic to publish messages to. \
        queueName and topicName must be set exclusively.
        """;

    public static final String CONNECTION_STRING_DESCRIPTION = """
        The connection string for a Service Bus \
         namespace or a specific Service Bus resource.
        """;
    public static final String SUBSCRIPTION_NAME_DESCRIPTION = "The name of the subscription in the topic to listen to.";

    public static final String SERDE_TYPE_DESCRIPTION = "The serializer/deserializer to use.";


    protected ServiceBusClientBuilder applyAuth(RunContext runContext, ServiceBusClientBuilder serviceBusClientBuilder) throws IllegalVariableEvaluationException {
        Optional<String> rConnectionString = runContext.render(this.connectionString).as(String.class);
        if (rConnectionString.isPresent()) {
            serviceBusClientBuilder.connectionString(rConnectionString.get());
        } else {
            serviceBusClientBuilder.credential(credentials(runContext));
        }

        return serviceBusClientBuilder;
    }
}