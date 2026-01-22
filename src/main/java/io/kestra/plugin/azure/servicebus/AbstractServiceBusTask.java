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
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.util.Optional;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractServiceBusTask extends AbstractAzureIdentityConnection {
    @NotNull
    @Schema
    protected Property<String> queueName;

    @NotNull
    @Schema()
    protected Property<String> topicName;

    @NotNull
    @Schema()
    protected Property<String> connectionString;

    protected static final ObjectMapper OBJECT_MAPPER = JacksonMapper.ofJson(false);


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
//