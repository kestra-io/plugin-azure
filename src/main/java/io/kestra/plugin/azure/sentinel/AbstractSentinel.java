package io.kestra.plugin.azure.sentinel;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
import lombok.Builder;
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
public abstract class AbstractSentinel extends AbstractAzureIdentityConnection {
    static final String API_VERSION = "2024-09-01";
    static final String DEFAULT_ARM_ENDPOINT = "https://management.azure.com";

    @NotNull
    @Schema(
        title = "Azure subscription ID",
        description = """
            ID of the Azure subscription that contains the Sentinel workspace."""
    )
    @PluginProperty(group = "connection")
    protected Property<String> subscriptionId;

    @NotNull
    @Schema(
        title = "Azure resource group",
        description = """
            Name of the resource group that contains the Sentinel workspace."""
    )
    @PluginProperty(group = "connection")
    protected Property<String> resourceGroup;

    @NotNull
    @Schema(
        title = "Sentinel workspace name",
        description = """
            ARM resource name of the Log Analytics workspace, not its customer GUID (the `workspaceId` used by `Query`)."""
    )
    @PluginProperty(group = "connection")
    protected Property<String> workspaceName;

    @Builder.Default
    @Schema(
        title = "Azure Resource Manager endpoint",
        description = """
            Base URL of the Azure Resource Manager API. Defaults to `https://management.azure.com` (public cloud).
            Override it for sovereign clouds, for example `https://management.usgovcloudapi.net` (Azure Government) or
            `https://management.chinacloudapi.cn` (Azure China). The access token scope is derived from this endpoint
            (`<endpoint>/.default`). Microsoft Entra sign-in uses the public authority
            unless the worker sets the `AZURE_AUTHORITY_HOST` environment variable, for example `https://login.microsoftonline.us`."""
    )
    @PluginProperty(group = "connection")
    protected Property<String> armEndpoint = Property.ofValue(DEFAULT_ARM_ENDPOINT);

    static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    static String required(RunContext context, Property<String> property, String name) throws Exception {
        return context.render(property).as(String.class).filter(s -> !s.isBlank())
            .orElseThrow(() -> new IllegalArgumentException(name + " is required"));
    }

    static String endpoint(RunContext context, Property<String> property, String defaultEndpoint) throws Exception {
        var rEndpoint = context.render(property).as(String.class).filter(s -> !s.isBlank()).orElse(defaultEndpoint);
        return rEndpoint.endsWith("/") ? rEndpoint.substring(0, rEndpoint.length() - 1) : rEndpoint;
    }

    protected URI incidentsUri(RunContext context, String suffix, Map<String, String> query) throws Exception {
        var path = "/subscriptions/" + encode(required(context, subscriptionId, "subscriptionId"))
            + "/resourceGroups/" + encode(required(context, resourceGroup, "resourceGroup"))
            + "/providers/Microsoft.OperationalInsights/workspaces/" + encode(required(context, workspaceName, "workspaceName"))
            + "/providers/Microsoft.SecurityInsights/incidents" + suffix;
        var uri = new StringBuilder(endpoint(context, armEndpoint, DEFAULT_ARM_ENDPOINT)).append(path).append("?api-version=").append(API_VERSION);
        query.forEach((key, value) -> uri.append('&').append(encode(key)).append('=').append(encode(value)));
        return URI.create(uri.toString());
    }

    protected Map<String, Object> armRequest(RunContext context, String method, URI uri, Map<String, Object> body) throws Exception {
        var rEndpoint = URI.create(endpoint(context, armEndpoint, DEFAULT_ARM_ENDPOINT));
        validateLink(rEndpoint, uri);
        var token = credentials(context).getToken(new TokenRequestContext().addScopes(rEndpoint + "/.default")).block();
        if (token == null) {
            throw new IllegalStateException("Failed to acquire an Azure Resource Manager access token");
        }
        var request = HttpRequest.builder().uri(uri).method(method)
            .addHeader("Authorization", "Bearer " + token.getToken())
            .addHeader("Content-Type", "application/json");
        if (body != null) {
            request.body(HttpRequest.JsonRequestBody.builder().content(body).build());
        }
        try (var client = HttpClient.builder().runContext(context).configuration(HttpConfiguration.builder().followRedirects(Property.ofValue(false)).build()).build()) {
            HttpResponse<Map<String, Object>> response = client.request(request.build());
            if (response.getBody() == null) {
                throw new IllegalStateException("Azure Sentinel returned an empty response");
            }
            return response.getBody();
        } catch (HttpClientResponseException e) {
            var detail = "";
            if (e.getResponse() != null && e.getResponse().getBody() instanceof byte[] bytes) {
                try {
                    var error = JacksonMapper.ofJson().readTree(bytes).path("error");
                    detail = ": " + error.path("code").asText("") + " " + error.path("message").asText("");
                } catch (IOException ignored) {
                    // Non-JSON error responses still report the HTTP status below.
                }
            }
            var status = e.getResponse() == null ? "unknown" : Integer.toString(e.getResponse().getStatus().getCode());
            throw new IllegalStateException("Azure Sentinel HTTP " + status + detail.replace(token.getToken(), "[redacted]"));
        }
    }

    private static void validateLink(URI endpoint, URI uri) {
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
        var rEndpoint = URI.create(endpoint(context, armEndpoint, DEFAULT_ARM_ENDPOINT));
        var visited = new HashSet<URI>();
        while (uri != null) {
            validateLink(rEndpoint, uri);
            if (!visited.add(uri)) {
                throw new IllegalStateException("Azure Sentinel returned a repeated pagination link");
            }
            var response = armRequest(context, "GET", uri, null);
            if (!consumer.accept(values(response))) {
                return;
            }
            var next = response.get("nextLink");
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
