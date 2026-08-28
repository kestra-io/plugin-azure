package io.kestra.plugin.azure.aifoundry;

import com.azure.core.credential.KeyCredential;
import com.azure.core.credential.TokenCredential;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractAiFoundryTask extends Task {

    @Schema(
        title = "Azure AI Foundry endpoint",
        description = "The Azure AI Foundry project or model endpoint URL."
    )
    @NotNull
    @PluginProperty(group = "connection")
    private Property<String> endpoint;

    @Schema(
        title = "Azure AI Foundry API key",
        description = "API key for API-key authentication. When omitted, DefaultAzureCredential (Entra ID) is used instead."
    )
    @ToString.Exclude
    @PluginProperty(group = "connection", secret = true)
    private Property<String> apiKey;

    @Schema(title = "Azure tenant ID", description = "Azure Entra tenant ID used with clientId and clientSecret for service principal authentication.")
    @PluginProperty(group = "connection")
    protected Property<String> tenantId;

    @Schema(title = "Azure client ID", description = "Client ID of the Azure app registration used with tenantId and clientSecret.")
    @PluginProperty(group = "connection")
    protected Property<String> clientId;

    @Schema(title = "Azure client secret", description = "Client secret of the Azure app registration used with tenantId and clientId.")
    @ToString.Exclude
    @PluginProperty(group = "connection", secret = true)
    protected Property<String> clientSecret;

    protected String getEndpoint(RunContext runContext)
        throws IllegalVariableEvaluationException {

        return runContext.render(this.endpoint)
            .as(String.class)
            .orElseThrow(
                () -> new IllegalArgumentException(
                    "endpoint is required. Set it to your Azure AI Foundry project endpoint."
                )
            );
    }

    protected String getApiKey(RunContext runContext)
        throws IllegalVariableEvaluationException {

        return runContext.render(this.apiKey)
            .as(String.class)
            .orElse(null);
    }

    protected KeyCredential getKeyCredential(RunContext runContext)
        throws IllegalVariableEvaluationException {

        String key = getApiKey(runContext);
        return (key != null && !key.isBlank()) ? new KeyCredential(key) : null;
    }

    protected TokenCredential getTokenCredential(RunContext runContext)
        throws IllegalVariableEvaluationException {

        return AiFoundryCredentials.tokenCredential(runContext, tenantId, clientId, clientSecret);
    }
}
