package io.kestra.plugin.azure.monitoring;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import com.azure.monitor.query.metrics.MetricsClient;
import com.azure.monitor.query.metrics.MetricsClientBuilder;
import com.azure.monitor.query.metrics.MetricsServiceVersion;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.http.client.HttpClientException;
import io.kestra.core.http.client.configurations.HttpConfiguration;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.AbstractAzureIdentityConnection;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractMonitoringTask extends AbstractAzureIdentityConnection {
    @Schema(
        title = "Azure Monitor Metrics regional endpoint",
        description = "Must be the regional endpoint (e.g. https://westeurope.metrics.monitor.azure.com)"
    )
    @NotNull
    protected Property<String> endpoint;

    protected MetricsClient queryClient(RunContext runContext) throws IllegalVariableEvaluationException {
        TokenCredential baseCredential = this.credentials(runContext);

        TokenCredential scopedCredential = requestContext -> {
            requestContext.setScopes(Collections.singletonList("https://metrics.monitor.azure.com/.default"));
            return baseCredential.getToken(requestContext);
        };

        return new MetricsClientBuilder()
            .credential(scopedCredential)
            .endpoint(runContext.render(endpoint).as(String.class).orElseThrow())
            .serviceVersion(MetricsServiceVersion.getLatest())
            .buildClient();
    }

    protected HttpResponse<Map<String, Object>> postToIngestion(RunContext runContext, String path, Map<String, Object> body) throws Exception {
        var rEndpoint = runContext.render(endpoint).as(String.class).orElseThrow();
        TokenCredential credential = this.credentials(runContext);

        AccessToken token = credential
            .getToken(new TokenRequestContext().addScopes("https://monitor.azure.com/.default"))
            .block();

        if (token == null) {
            throw new IllegalStateException("Failed to acquire Azure access token for ingestion");
        }

        URI uri = URI.create(rEndpoint + path);

        HttpRequest.HttpRequestBuilder builder = HttpRequest.builder()
            .uri(uri)
            .method("POST")
            .addHeader("Authorization", "Bearer " + token.getToken())
            .addHeader("Content-Type", "application/json")
            .body(HttpRequest.JsonRequestBody.builder().content(body).build());

        try (HttpClient client = HttpClient.builder().runContext(runContext).configuration(HttpConfiguration.builder().build()).build()) {
            return client.request(builder.build());
        } catch (IOException | HttpClientException e) {
            throw new RuntimeException("Failed to post data to Azure Monitor ingestion API", e);
        }
    }
}
