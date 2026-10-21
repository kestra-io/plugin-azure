package io.kestra.plugin.azure.devops.pipelines;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import org.apache.commons.lang3.StringUtils;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import com.azure.identity.ClientCertificateCredentialBuilder;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.fasterxml.jackson.databind.JsonNode;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.http.client.HttpClientException;
import io.kestra.core.http.client.HttpClientResponseException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.azure.shared.AzureIdentityConnectionInterface;

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
public abstract class AbstractAzureDevOpsTask extends Task implements AzureIdentityConnectionInterface {
    protected static final String API_VERSION = "7.1";
    protected static final String AZURE_DEVOPS_SCOPE = "499b84ac-1321-427f-aa17-267ca6975798/.default";

    @Schema(
        title = "Azure DevOps organization",
        description = """
            The name of the Azure DevOps organization or Azure DevOps Server collection.
            """
    )
    @NotNull
    @PluginProperty(group = "connection")
    protected Property<String> organization;

    @Schema(
        title = "Azure DevOps project",
        description = """
            The name or ID of the Azure DevOps project containing the pipeline.
            """
    )
    @NotNull
    @PluginProperty(group = "main")
    protected Property<String> project;

    @Schema(
        title = "Base URL",
        description = """
            Base URL of the Azure DevOps instance. Defaults to `https://dev.azure.com`.
            Configure this property when connecting to an on-premises Azure DevOps Server.
            """
    )
    @Builder.Default
    @PluginProperty(group = "connection")
    protected Property<String> baseUrl = Property.ofValue("https://dev.azure.com");

    @Schema(
        title = "Personal Access Token",
        description = """
            Azure DevOps Personal Access Token (PAT) with `Build: Read & execute` scope.
            If omitted, Microsoft Entra ID service principal credentials must be configured.
            """
    )
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    @PluginProperty(group = "connection", secret = true)
    protected Property<String> personalAccessToken;

    @Schema(
        title = "Azure tenant ID",
        description = """
            Azure Entra tenant ID used for service principal authentication.
            """
    )
    @PluginProperty(group = "connection")
    protected Property<String> tenantId;

    @Schema(
        title = "Azure client ID",
        description = """
            Client ID of the Azure service principal.
            """
    )
    @PluginProperty(group = "connection")
    protected Property<String> clientId;

    @Schema(
        title = "Azure client secret",
        description = """
            Client secret of the Azure service principal.
            """
    )
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    @PluginProperty(group = "connection", secret = true)
    protected Property<String> clientSecret;

    @Schema(
        title = "PEM certificate",
        description = """
            PEM certificate content for certificate-based authentication.
            """
    )
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    @PluginProperty(group = "connection", secret = true)
    protected Property<String> pemCertificate;

    public TokenCredential credentials(RunContext runContext) throws IllegalVariableEvaluationException {
        var rTenantId = runContext.render(this.tenantId).as(String.class).orElse(null);
        var rClientId = runContext.render(this.clientId).as(String.class).orElse(null);
        var rClientSecret = runContext.render(this.clientSecret).as(String.class).orElse(null);
        var rPemCert = runContext.render(this.pemCertificate).as(String.class).orElse(null);

        if (StringUtils.isNotBlank(rClientSecret)) {
            runContext.logger().debug("Azure DevOps authentication using Client Secret Credentials");
            return new ClientSecretCredentialBuilder()
                .tenantId(rTenantId)
                .clientId(rClientId)
                .clientSecret(rClientSecret)
                .build();
        }
        if (StringUtils.isNotBlank(rPemCert)) {
            runContext.logger().debug("Azure DevOps authentication using Client Certificate Credentials");
            return new ClientCertificateCredentialBuilder()
                .tenantId(rTenantId)
                .clientId(rClientId)
                .pemCertificate(new ByteArrayInputStream(rPemCert.getBytes(StandardCharsets.UTF_8)))
                .build();
        }
        runContext.logger().debug("Azure DevOps authentication using Default Azure Credentials");
        var builder = new DefaultAzureCredentialBuilder();
        if (rTenantId != null) {
            builder.tenantId(rTenantId);
        }
        return builder.build();
    }

    protected String buildProjectApiUrl(RunContext runContext) throws IllegalVariableEvaluationException {
        var rBaseUrl = runContext.render(this.baseUrl).as(String.class).orElse("https://dev.azure.com");
        if (rBaseUrl.endsWith("/")) {
            rBaseUrl = rBaseUrl.substring(0, rBaseUrl.length() - 1);
        }

        var rOrganization = runContext.render(this.organization)
            .as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("organization is required"));
        var rProject = runContext.render(this.project)
            .as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("project is required"));

        var encodedOrg = URLEncoder.encode(rOrganization, StandardCharsets.UTF_8).replace("+", "%20");
        var encodedProj = URLEncoder.encode(rProject, StandardCharsets.UTF_8).replace("+", "%20");

        return "%s/%s/%s/_apis".formatted(rBaseUrl, encodedOrg, encodedProj);
    }

    protected String getAuthorizationHeader(RunContext runContext, URI uri) throws Exception {
        var rPat = runContext.render(this.personalAccessToken).as(String.class).orElse(null);
        if (rPat != null && !rPat.isBlank()) {
            var token = ":" + rPat;
            return "Basic " + Base64.getEncoder().encodeToString(token.getBytes(StandardCharsets.UTF_8));
        }

        var rTenantId = runContext.render(this.tenantId).as(String.class).orElse(null);
        var rClientId = runContext.render(this.clientId).as(String.class).orElse(null);
        var rClientSecret = runContext.render(this.clientSecret).as(String.class).orElse(null);
        var rPemCert = runContext.render(this.pemCertificate).as(String.class).orElse(null);

        // Microsoft Entra ID authentication (Service Principal or Managed Identity)
        assertEntraTokenTarget(uri);
        TokenCredential cred;
        if (StringUtils.isNotBlank(rClientSecret)) {
            runContext.logger().debug("Azure DevOps authentication using Client Secret Credentials");
            cred = new ClientSecretCredentialBuilder()
                .tenantId(rTenantId)
                .clientId(rClientId)
                .clientSecret(rClientSecret)
                .build();
        } else if (StringUtils.isNotBlank(rPemCert)) {
            runContext.logger().debug("Azure DevOps authentication using Client Certificate Credentials");
            cred = new ClientCertificateCredentialBuilder()
                .tenantId(rTenantId)
                .clientId(rClientId)
                .pemCertificate(new ByteArrayInputStream(rPemCert.getBytes(StandardCharsets.UTF_8)))
                .build();
        } else {
            runContext.logger().debug("Azure DevOps authentication using Default Azure Credentials (Managed Identity)");
            var builder = new DefaultAzureCredentialBuilder();
            if (rTenantId != null) {
                builder.tenantId(rTenantId);
            }
            cred = builder.build();
        }

        AccessToken token = cred.getToken(new TokenRequestContext().addScopes(AZURE_DEVOPS_SCOPE)).block();
        if (token == null) {
            throw new IllegalStateException("Failed to acquire Azure Entra ID access token for Azure DevOps");
        }
        return "Bearer " + token.getToken();
    }

    // Entra ID tokens are only valid for Azure DevOps Services, never send them to another host.
    private static void assertEntraTokenTarget(URI uri) {
        String host = uri.getHost();
        boolean allowed = "https".equalsIgnoreCase(uri.getScheme())
            && host != null
            && (host.equalsIgnoreCase("dev.azure.com") || host.toLowerCase().endsWith(".dev.azure.com"));
        if (!allowed) {
            throw new IllegalArgumentException(
                "Microsoft Entra ID authentication is only supported for https://dev.azure.com, use 'personalAccessToken' for other hosts."
            );
        }
    }

    protected HttpResponse<String> executeRequest(RunContext runContext, String authHeader, String method, URI uri, Object body) throws Exception {
        var builder = HttpRequest.builder()
            .uri(uri)
            .method(method)
            .addHeader("Authorization", authHeader)
            .addHeader("Accept", "application/json");

        if (body != null) {
            builder.addHeader("Content-Type", "application/json");
            builder.body(HttpRequest.JsonRequestBody.builder().content(body).build());
        }

        try (HttpClient client = HttpClient.builder().runContext(runContext).build()) {
            return client.request(builder.build(), String.class);
        } catch (HttpClientResponseException e) {
            int statusCode = e.getResponse() != null ? e.getResponse().getStatus().getCode() : -1;
            String responseBody = e.getResponse() != null && e.getResponse().getBody() != null ? e.getResponse().getBody().toString() : "";
            String hint = switch (statusCode) {
                case 401, 403 -> "Please verify that your Personal Access Token (PAT) has the 'Build: Read & execute' scope or check Entra ID service principal role assignments.";
                case 404 -> "Please verify that the organization, project, and pipeline/run ID exist and are accessible.";
                default -> "Please verify the request payload and Azure DevOps service status.";
            };
            throw new IllegalStateException(
                "Azure DevOps API request [%s %s] failed with HTTP %d: %s. %s".formatted(method, uri, statusCode, responseBody, hint),
                e
            );
        } catch (HttpClientException | IOException e) {
            throw new RuntimeException("Failed to execute Azure DevOps HTTP request to " + uri, e);
        }
    }

    protected HttpResponse<String> executeRequest(RunContext runContext, String method, URI uri, Object body) throws Exception {
        var authHeader = getAuthorizationHeader(runContext, uri);
        return executeRequest(runContext, authHeader, method, uri, body);
    }

    protected PipelineRun parseRunResponse(String json, URI uri) throws Exception {
        if (json == null || json.isBlank()) {
            throw new IllegalStateException("Empty response from Azure DevOps API for " + uri);
        }
        JsonNode root = JacksonMapper.ofJson().readTree(json);
        return PipelineRun.fromJson(root);
    }

    protected PipelineRun parseRunResponse(String json) throws Exception {
        return parseRunResponse(json, null);
    }
}
