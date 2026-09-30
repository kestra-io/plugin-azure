package io.kestra.plugin.azure.aifoundry;

import com.azure.core.credential.TokenCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.DefaultAzureCredentialBuilder;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;

final class AiFoundryCredentials {
    private AiFoundryCredentials() {
    }

    static TokenCredential tokenCredential(
        RunContext runContext,
        Property<String> tenantId,
        Property<String> clientId,
        Property<String> clientSecret) throws IllegalVariableEvaluationException {
        String renderedTenantId = render(runContext, tenantId);
        String renderedClientId = render(runContext, clientId);
        String renderedClientSecret = render(runContext, clientSecret);

        boolean hasTenantId = hasText(renderedTenantId);
        boolean hasClientId = hasText(renderedClientId);
        boolean hasClientSecret = hasText(renderedClientSecret);

        if (hasTenantId || hasClientId || hasClientSecret) {
            if (!hasTenantId || !hasClientId || !hasClientSecret) {
                throw new IllegalArgumentException(
                    "tenantId, clientId, and clientSecret must all be set to use service principal authentication."
                );
            }

            return new ClientSecretCredentialBuilder()
                .tenantId(renderedTenantId)
                .clientId(renderedClientId)
                .clientSecret(renderedClientSecret)
                .build();
        }

        return new DefaultAzureCredentialBuilder().build();
    }

    private static String render(RunContext runContext, Property<String> property)
        throws IllegalVariableEvaluationException {

        return runContext.render(property).as(String.class).orElse(null);
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}
