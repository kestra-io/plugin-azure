package io.kestra.plugin.azure.sentinel;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenRequestContext;

import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.http.client.HttpClientResponseException;
import io.kestra.core.http.client.configurations.HttpConfiguration;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.azure.shared.AbstractAzureIdentityConnection;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractSentinel extends AbstractAzureIdentityConnection {
    static final String API_VERSION = "2024-09-01";
    static final String ARM_SCOPE = "https://management.azure.com/.default";

    @NotNull
    @Schema(title = "Azure subscription ID")
    @PluginProperty(group = "connection")
    protected Property<String> subscriptionId;

    @NotNull
    @Schema(title = "Azure resource group")
    @PluginProperty(group = "connection")
    protected Property<String> resourceGroup;

    @NotNull
    @Schema(title = "Sentinel workspace name", description = "ARM resource name of the Log Analytics workspace, not its customer GUID (workspaceId used by Query).")
    @PluginProperty(group = "connection")
    protected Property<String> workspaceName;

    /** Test seam; production requests always target Azure Resource Manager. */
    protected URI armEndpoint() {
        return URI.create("https://management.azure.com");
    }

    static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    static String required(RunContext context, Property<String> property, String name) throws Exception {
        return context.render(property).as(String.class).filter(s -> !s.isBlank())
            .orElseThrow(() -> new IllegalArgumentException(name + " is required"));
    }

    protected URI incidentsUri(RunContext context, String suffix, Map<String, String> query) throws Exception {
        String path = "/subscriptions/" + encode(required(context, subscriptionId, "subscriptionId"))
            + "/resourceGroups/" + encode(required(context, resourceGroup, "resourceGroup"))
            + "/providers/Microsoft.OperationalInsights/workspaces/" + encode(required(context, workspaceName, "workspaceName"))
            + "/providers/Microsoft.SecurityInsights/incidents" + suffix;
        StringBuilder uri = new StringBuilder(armEndpoint().toString()).append(path).append("?api-version=").append(API_VERSION);
        query.forEach((key, value) -> uri.append('&').append(encode(key)).append('=').append(encode(value)));
        return URI.create(uri.toString());
    }

    protected Map<String, Object> armRequest(RunContext context, String method, URI uri, Map<String, Object> body) throws Exception {
        validateLink(uri);
        AccessToken token = credentials(context).getToken(new TokenRequestContext().addScopes(ARM_SCOPE)).block();
        if (token == null) {
            throw new IllegalStateException("Failed to acquire an Azure Resource Manager access token");
        }
        var request = HttpRequest.builder().uri(uri).method(method)
            .addHeader("Authorization", "Bearer " + token.getToken())
            .addHeader("Content-Type", "application/json");
        if (body != null) {
            request.body(HttpRequest.JsonRequestBody.builder().content(body).build());
        }
        try (HttpClient client = HttpClient.builder().runContext(context).configuration(HttpConfiguration.builder().followRedirects(Property.ofValue(false)).build()).build()) {
            HttpResponse<Map<String, Object>> response = client.request(request.build());
            if (response.getBody() == null) {
                throw new IllegalStateException("Azure Sentinel returned an empty response");
            }
            return response.getBody();
        } catch (HttpClientResponseException e) {
            String detail = "";
            if (e.getResponse() != null && e.getResponse().getBody() instanceof byte[] bytes) {
                try {
                    var error = JacksonMapper.ofJson().readTree(bytes).path("error");
                    detail = ": " + error.path("code").asText("") + " " + error.path("message").asText("");
                } catch (java.io.IOException ignored) {
                    // Non-JSON error responses still report the HTTP status below.
                }
            }
            String status = e.getResponse() == null ? "unknown" : Integer.toString(e.getResponse().getStatus().getCode());
            throw new IllegalStateException("Azure Sentinel HTTP " + status + detail.replace(token.getToken(), "[redacted]"));
        }
    }

    private void validateLink(URI uri) {
        URI endpoint = armEndpoint();
        if (
            !Objects.equals(endpoint.getScheme(), uri.getScheme()) || !Objects.equals(endpoint.getHost(), uri.getHost())
                || endpoint.getPort() != uri.getPort() || uri.getUserInfo() != null || uri.getFragment() != null
        ) {
            throw new IllegalArgumentException("Azure Sentinel pagination link must use the ARM origin");
        }
    }

    @FunctionalInterface
    interface PageConsumer {
        boolean accept(List<Map<String, Object>> page) throws Exception;
    }

    protected void pages(RunContext context, URI uri, PageConsumer consumer) throws Exception {
        Set<URI> visited = new HashSet<>();
        while (uri != null) {
            validateLink(uri);
            if (!visited.add(uri)) {
                throw new IllegalStateException("Azure Sentinel returned a repeated pagination link");
            }
            Map<String, Object> response = armRequest(context, "GET", uri, null);
            if (!consumer.accept(values(response))) {
                return;
            }
            Object next = response.get("nextLink");
            if (next != null && !(next instanceof String)) {
                throw new IllegalStateException("Azure Sentinel returned an invalid nextLink");
            }
            uri = next == null || ((String) next).isBlank() ? null : uri.resolve((String) next);
        }
    }

    @SuppressWarnings("unchecked")
    static List<Map<String, Object>> values(Map<String, Object> response) {
        if (!(response.get("value") instanceof List<?> values) || values.stream().anyMatch(v -> !(v instanceof Map<?, ?>))) {
            throw new IllegalStateException("Azure Sentinel response must contain a value array of objects");
        }
        return (List<Map<String, Object>>) (List<?>) values;
    }
}
