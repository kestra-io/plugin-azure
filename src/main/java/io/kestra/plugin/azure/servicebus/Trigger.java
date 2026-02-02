package io.kestra.plugin.azure.servicebus;

import com.azure.messaging.servicebus.models.ServiceBusReceiveMode;
import com.azure.messaging.servicebus.models.SubQueue;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.time.Duration;
import java.util.Optional;

import static io.kestra.plugin.azure.servicebus.AbstractServiceBusTask.*;
import static io.kestra.plugin.azure.servicebus.Consume.*;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Poll Azure Service Bus for messages",
    description = "Polling trigger that reads from a queue or topic subscription using connection string or Azure AD credentials. Polls every 60 seconds and stops each cycle when maxReceiveDuration elapses or no messages are returned."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: service_bus_listen
                namespace: company.team

                tasks:
                  - id: log_messages
                    type: io.kestra.plugin.core.log.Log
                    message: "Trigger body: {{trigger.body}}."

                triggers:
                  - id: watch
                    type: io.kestra.plugin.azure.servicebus.Trigger
                    maxReceiveDuration: PT30S
                    maxMessages: 100
                    queueName: your-queue-name
                    tenantId: "{{ secret('AZURE_TENANT_ID') }}"
                    clientId: "{{ secret('AZURE_CLIENT_ID') }}"
                    clientSecret: "{{ secret('AZURE_CLIENT_SECRET') }}"
                """
        )
    }
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Consume.Output> {
    @Schema(description = QUEUE_NAME_DESCRIPTION)
    private Property<String> queueName;

    @Schema(description = TOPIC_NAME_DESCRIPTION)
    private Property<String> topicName;

    @Schema(description = CONNECTION_STRING_DESCRIPTION)
    private Property<String> connectionString;

    @Schema(description = SUBSCRIPTION_NAME_DESCRIPTION)
    private Property<String> subscriptionName;

    @Schema(description = RECEIVE_MODE_DESCRIPTION)
    private Property<ServiceBusReceiveMode> receiveMode;

    @Schema(description = SUB_QUEUE_DESCRIPTION)
    private Property<SubQueue> subQueue;

    @Builder.Default
    @NotNull
    @Schema(description = SERDE_TYPE_DESCRIPTION)
    private Property<SerdeType> serdeType = Property.ofValue(SerdeType.STRING);

    @Schema(description = "Azure Active Directory tenant ID used when authenticating without a connection string")
    private Property<String> tenantId;

    @Schema(description = "Client ID for the Azure app registration used with tenantId")
    private Property<String> clientId;

    @Schema(description = "Client secret for the Azure app registration; not needed when using pemCertificate")
    private Property<String> clientSecret;

    @Schema(description = "PEM certificate content for certificate-based authentication")
    private Property<String> pemCertificate;

    @Schema(description = MAX_MESSAGES_DESCRIPTION)
    protected Property<Integer> maxMessages;

    @NotNull
    @Schema(description = MAX_RECEIVE_DURATION_DESCRIPTION)
    protected Property<Duration> maxReceiveDuration;

    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();

        Consume consume = Consume.builder()
            .topicName(topicName)
            .queueName(queueName)
            .connectionString(connectionString)
            .subscriptionName(subscriptionName)
            .clientId(clientId)
            .clientSecret(clientSecret)
            .tenantId(tenantId)
            .pemCertificate(pemCertificate)
            .subQueue(subQueue)
            .receiveMode(receiveMode)
            .maxMessages(maxMessages)
            .maxReceiveDuration(maxReceiveDuration)
            .serdeType(serdeType)
            .build();

        Consume.Output run = consume.run(runContext);

        if (run.getCount() == 0) {
            return Optional.empty();
        }

        return Optional.ofNullable(
            TriggerService.generateExecution(this, conditionContext, context, run)
        );
    }
}
