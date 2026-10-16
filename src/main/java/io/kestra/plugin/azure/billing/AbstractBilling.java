package io.kestra.plugin.azure.billing;

import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenRequestContext;
import com.google.common.annotations.VisibleForTesting;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.shared.AbstractAzureIdentityConnection;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

/**
 * Base class for the Azure billing tasks. Calls the Azure Resource Manager REST API (Cost Management and Consumption)
 * with a bearer token from the shared Azure identity connection.
 */
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractBilling extends AbstractAzureIdentityConnection {
    static final String MANAGEMENT_ENDPOINT = "https://management.azure.com";
    private static final String MANAGEMENT_SCOPE = MANAGEMENT_ENDPOINT + "/.default";
    private static final Duration TOKEN_TIMEOUT = Duration.ofSeconds(60);

    @Schema(
        title = "Scope",
        description = """
            Azure scope the costs or budgets belong to, for example `/subscriptions/{subscriptionId}`,
            `/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}`,
            `/providers/Microsoft.Management/managementGroups/{managementGroupId}` or
            `/providers/Microsoft.Billing/billingAccounts/{billingAccountId}`."""
    )
    @NotNull
    @PluginProperty(group = "main")
    protected Property<String> scope;

    protected String renderScope(RunContext runContext) throws IllegalVariableEvaluationException {
        var rScope = runContext.render(scope).as(String.class).orElseThrow().trim();
        if (!rScope.startsWith("/subscriptions/") && !rScope.startsWith("/providers/")) {
            throw new IllegalArgumentException("scope must start with /subscriptions/ or /providers/, got '" + rScope + "'");
        }
        return rScope.endsWith("/") ? rScope.substring(0, rScope.length() - 1) : rScope;
    }

    static LocalDate parseDate(String name, String value) {
        try {
            return LocalDate.parse(value);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException(name + " must be a date formatted as yyyy-MM-dd, got '" + value + "'", e);
        }
    }

    static int maxPages(RunContext runContext, Property<Integer> maxPages) throws IllegalVariableEvaluationException {
        int value = runContext.render(maxPages).as(Integer.class).orElseThrow();
        if (value < 1) {
            throw new IllegalArgumentException("maxPages must be at least 1, got " + value);
        }
        return value;
    }

    /** Opens the Azure Resource Manager client of one task run; the access token is fetched once here. */
    @VisibleForTesting
    BillingApi api(RunContext runContext) throws Exception {
        AccessToken token = credentials(runContext).getToken(new TokenRequestContext().addScopes(MANAGEMENT_SCOPE)).block(TOKEN_TIMEOUT);
        if (token == null) {
            throw new IllegalStateException("Failed to acquire an Azure access token for " + MANAGEMENT_SCOPE);
        }
        return new BillingApi(runContext, token.getToken());
    }
}
