package io.kestra.plugin.azure.monitoring;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Map;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import com.azure.monitor.ingestion.LogsIngestionClient;
import com.azure.monitor.ingestion.LogsIngestionClientBuilder;
import com.azure.monitor.query.metrics.MetricsClient;
import com.azure.monitor.query.metrics.MetricsClientBuilder;
import com.azure.monitor.query.metrics.MetricsServiceVersion;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.http.client.HttpClientException;
import io.kestra.core.http.client.configurations.HttpConfiguration;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
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
public abstract class AbstractMonitoringTask extends AbstractAzureIdentityConnection {
    @Schema(
        title = "Azure Monitor regional endpoint",
        description = "For queries, the regional metrics endpoint, e.g. https://westeurope.metrics.monitor.azure.com. For ingestion, the Data Collection Endpoint, e.g. https://my-dce-a1b2.westeurope.ingest.monitor.azure.com"
    )
    @NotNull
    @PluginProperty(group = "main")
    protected Property<String> endpoint;

    protected MetricsClient queryClient(RunContext runContext) throws IllegalVariableEvaluationException {
        TokenCredential baseCredential = this.credentials(runContext);

        TokenCredential scopedCredential = requestContext ->
        {
            requestContext.setScopes(Collections.singletonList("https://metrics.monitor.azure.com/.default"));
            return baseCredential.getToken(requestContext);
        };

        return new MetricsClientBuilder()
            .credential(scopedCredential)
            .endpoint(runContext.render(endpoint).as(String.class).orElseThrow())
            .serviceVersion(MetricsServiceVersion.getLatest())
            .buildClient();
    }

    /** Overridable so tests can swap the client, the SDK exposes no base URL seam of its own. */
    protected LogsIngestionClient ingestionClient(RunContext runContext) throws IllegalVariableEvaluationException {
        // the SDK sets the monitor.azure.com ingestion audience itself, so no scope wrapper is needed here
        return new LogsIngestionClientBuilder()
            .credential(this.credentials(runContext))
            .endpoint(runContext.render(endpoint).as(String.class).orElseThrow())
            .buildClient();
    }

    /**
     * Pre-SDK behaviour, kept for any path the ingestion client cannot address so Azure stays the authority on
     * what is a valid endpoint rather than a regex here.
     */
    protected HttpResponse<Map<String, Object>> postVerbatim(RunContext runContext, String path, Map<String, Object> body) throws Exception {
        var rEndpoint = runContext.render(endpoint).as(String.class).orElseThrow();

        AccessToken token = this.credentials(runContext)
            .getToken(new TokenRequestContext().addScopes("https://monitor.azure.com/.default"))
            .block();

        if (token == null) {
            throw new IllegalStateException("Failed to acquire Azure access token for ingestion");
        }

        HttpRequest.HttpRequestBuilder builder = HttpRequest.builder()
            .uri(URI.create("%s%s".formatted(rEndpoint, path)))
            .method("POST")
            .addHeader("Authorization", "Bearer %s".formatted(token.getToken()))
            .addHeader("Content-Type", "application/json")
            .body(HttpRequest.JsonRequestBody.builder().content(body).build());

        try (HttpClient client = HttpClient.builder().runContext(runContext).configuration(HttpConfiguration.builder().build()).build()) {
            return client.request(builder.build());
        } catch (IOException | HttpClientException e) {
            throw new RuntimeException("Failed to post data to Azure Monitor ingestion API", e);
        }
    }
}
