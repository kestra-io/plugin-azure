package io.kestra.plugin.azure.aifoundry;

import org.apache.commons.lang3.StringUtils;

import com.azure.core.credential.KeyCredential;
import com.azure.core.credential.TokenCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;

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
        title = "Azure AI Foundry endpoint.",
        description = "The Azure AI Foundry project or model endpoint."
    )
    @NotNull
    private Property<String> endpoint;

    @Schema(
        title = "Azure AI Foundry API key.",
        description = "API key used when API-key authentication is configured."
    )
    @PluginProperty(secret = true)
    private Property<String> apiKey;

    protected String getEndpoint(RunContext runContext)
        throws IllegalVariableEvaluationException {

        return runContext.render(this.endpoint)
            .as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("endpoint is required"));
    }

    protected String getApiKey(RunContext runContext)
        throws IllegalVariableEvaluationException {

        return runContext.render(this.apiKey)
            .as(String.class)
            .orElse(null);
    }

    protected KeyCredential getKeyCredential(RunContext runContext)
        throws IllegalVariableEvaluationException {

        String apiKey = getApiKey(runContext);

        return StringUtils.isNotBlank(apiKey)
            ? new KeyCredential(apiKey)
            : null;
    }

    protected TokenCredential getTokenCredential(RunContext runContext)
        throws IllegalVariableEvaluationException {

        if (StringUtils.isNotBlank(getApiKey(runContext))) {
            return null;
        }

        return new DefaultAzureCredentialBuilder().build();
    }
}