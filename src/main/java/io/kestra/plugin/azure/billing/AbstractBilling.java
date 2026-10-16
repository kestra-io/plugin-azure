package io.kestra.plugin.azure.billing;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.regex.Pattern;

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
    static final String MANAGEMENT_HOST = "management.azure.com";
    static final String MANAGEMENT_ENDPOINT = "https://" + MANAGEMENT_HOST;
    private static final String MANAGEMENT_SCOPE = MANAGEMENT_ENDPOINT + "/.default";
    static final int DEFAULT_MAX_PAGES = 10;
    private static final Pattern SCOPE_PATTERN = Pattern.compile("^/(subscriptions|providers)(/[A-Za-z0-9._:()-]+)+$");
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
        var rScope = runContext.render(scope).as(String.class).orElseThrow(() -> new IllegalArgumentException("scope is required")).trim();
        return validateScope(rScope);
    }

    static String validateScope(String value) {
        var rScope = value.endsWith("/") ? value.substring(0, value.length() - 1) : value;
        boolean dotSegment = Arrays.stream(rScope.split("/")).anyMatch(segment -> segment.equals(".") || segment.equals(".."));
        if (!SCOPE_PATTERN.matcher(rScope).matches() || dotSegment) {
            throw new IllegalArgumentException(
                "scope must start with /subscriptions/ or /providers/ and contain only path segments made of letters, digits and . _ : ( ) -, got '" + value + "'"
            );
        }
        return rScope;
    }

    /** Only follow a {@code nextLink} to Azure Resource Manager over https, so the bearer token is never sent elsewhere. */
    static String requireManagementUrl(String link) {
        URI uri;
        try {
            uri = URI.create(link);
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("Azure returned an invalid nextLink: '" + link + "'", e);
        }
        if (!"https".equalsIgnoreCase(uri.getScheme()) || !MANAGEMENT_HOST.equalsIgnoreCase(uri.getHost()) || uri.getPort() != -1 || uri.getUserInfo() != null) {
            throw new IllegalStateException("Refusing to follow nextLink outside https://" + MANAGEMENT_HOST + ": '" + link + "'");
        }
        return link;
    }

    static LocalDate parseDate(String name, String value) {
        try {
            return LocalDate.parse(value);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException(name + " must be a date formatted as yyyy-MM-dd, got '" + value + "'", e);
        }
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
