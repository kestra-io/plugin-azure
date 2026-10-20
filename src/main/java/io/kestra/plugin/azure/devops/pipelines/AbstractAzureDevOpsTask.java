package io.kestra.plugin.azure.devops.pipelines;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
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
        String tenant = runContext.render(this.tenantId).as(String.class).orElse(null);
        String client = runContext.render(this.clientId).as(String.class).orElse(null);
        String secret = runContext.render(this.clientSecret).as(String.class).orElse(null);
        String cert = runContext.render(this.pemCertificate).as(String.class).orElse(null);

        if (StringUtils.isNotBlank(secret)) {
            runContext.logger().info("Authentication is using Client Secret Credentials");
            return new ClientSecretCredentialBuilder()
                .tenantId(tenant)
                .clientId(client)
                .clientSecret(secret)
                .build();
        }
        if (StringUtils.isNotBlank(cert)) {
            runContext.logger().info("Authentication is using Client Certificate Credentials");
            return new ClientCertificateCredentialBuilder()
                .tenantId(tenant)
                .clientId(client)
                .pemCertificate(new ByteArrayInputStream(cert.getBytes(StandardCharsets.UTF_8)))
                .build();
        }
        runContext.logger().info("Authentication is using Default Azure Credentials");
        return new DefaultAzureCredentialBuilder()
            .tenantId(tenant)
            .build();
    }

    protected String buildProjectApiUrl(RunContext runContext) throws IllegalVariableEvaluationException {
        String base = runContext.render(this.baseUrl).as(String.class).orElse("https://dev.azure.com");
        if (base.endsWith("/")) {
            base = base.substring(0, base.length() - 1);
        }

        String org = runContext.render(this.organization).as(String.class).orElseThrow();
        String proj = runContext.render(this.project).as(String.class).orElseThrow();

        return "%s/%s/%s/_apis".formatted(base, org, proj);
    }

    protected String getAuthorizationHeader(RunContext runContext) throws Exception {
        String pat = runContext.render(this.personalAccessToken).as(String.class).orElse(null);
        if (pat != null && !pat.isBlank()) {
            String token = ":" + pat;
            return "Basic " + Base64.getEncoder().encodeToString(token.getBytes(StandardCharsets.UTF_8));
        }

        if (this.tenantId != null && this.clientId != null) {
            AccessToken token = this.credentials(runContext)
                .getToken(new TokenRequestContext().addScopes(AZURE_DEVOPS_SCOPE))
                .block();

            if (token == null) {
                throw new IllegalStateException("Failed to acquire Azure Entra ID access token for Azure DevOps");
            }
            return "Bearer " + token.getToken();
        }

        throw new IllegalArgumentException(
            "Either 'personalAccessToken' or Azure service principal credentials ('tenantId', 'clientId', 'clientSecret') must be configured."
        );
    }

    protected HttpResponse<String> executeRequest(RunContext runContext, String method, URI uri, Object body) throws Exception {
        String authHeader = getAuthorizationHeader(runContext);

        HttpRequest.HttpRequestBuilder builder = HttpRequest.builder()
            .uri(uri)
            .method(method)
            .addHeader("Authorization", authHeader)
            .addHeader("Accept", "application/json");

        if (body != null) {
            builder.addHeader("Content-Type", "application/json");
            builder.body(HttpRequest.JsonRequestBody.builder().content(body).build());
        }

        try (HttpClient client = HttpClient.builder().runContext(runContext).build()) {
            HttpResponse<String> response = client.request(builder.build(), String.class);
            if (response.getStatus().getCode() >= 400) {
                throw new IllegalStateException(
                    "Azure DevOps API request [%s %s] failed with HTTP %d: %s".formatted(
                        method,
                        uri,
                        response.getStatus().getCode(),
                        response.getBody()
                    )
                );
            }
            return response;
        } catch (HttpClientException | IOException e) {
            throw new RuntimeException("Failed to execute Azure DevOps HTTP request to " + uri, e);
        }
    }

    protected PipelineRun parseRunResponse(String json) throws Exception {
        JsonNode root = JacksonMapper.ofJson().readTree(json);
        return PipelineRun.fromJson(root);
    }
}
