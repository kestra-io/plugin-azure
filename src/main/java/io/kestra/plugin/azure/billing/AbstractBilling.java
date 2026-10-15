package io.kestra.plugin.azure.billing;

import java.net.URI;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.Map;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenRequestContext;
import com.google.common.annotations.VisibleForTesting;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.http.client.HttpClientResponseException;
import io.kestra.core.http.client.configurations.HttpConfiguration;
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

    /**
     * Sends one request to Azure Resource Manager and returns the JSON body. {@code body} is null for a GET.
     */
    @VisibleForTesting
    @SuppressWarnings("unchecked")
    Map<String, Object> send(RunContext runContext, String method, String url, Map<String, Object> body) throws Exception {
        AccessToken token = credentials(runContext).getToken(new TokenRequestContext().addScopes(MANAGEMENT_SCOPE)).block();
        if (token == null) {
            throw new IllegalStateException("Failed to acquire an Azure access token for " + MANAGEMENT_SCOPE);
        }

        var request = HttpRequest.builder()
            .uri(URI.create(url))
            .method(method)
            .addHeader("Authorization", "Bearer " + token.getToken())
            .addHeader("Content-Type", "application/json");
        if (body != null) {
            request.body(HttpRequest.JsonRequestBody.builder().content(body).build());
        }

        try (var client = HttpClient.builder().runContext(runContext).configuration(HttpConfiguration.builder().build()).build()) {
            Map<String, Object> response = client.request(request.build(), Map.class).getBody();
            return response == null ? Map.of() : response;
        } catch (HttpClientResponseException e) {
            var status = e.getResponse() == null ? "unknown" : String.valueOf(e.getResponse().getStatus().getCode());
            throw new IllegalStateException("Azure request " + method + " " + url + " failed with HTTP " + status + ": " + e.getMessage(), e);
        }
    }
}
