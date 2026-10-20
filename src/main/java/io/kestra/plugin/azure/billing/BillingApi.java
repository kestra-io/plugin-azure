package io.kestra.plugin.azure.billing;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Map;

import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.http.client.HttpClientResponseException;
import io.kestra.core.http.client.configurations.HttpConfiguration;
import io.kestra.core.http.client.configurations.TimeoutConfiguration;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;

/** Azure Resource Manager client for one task run: one access token, one HTTP client, retries on throttling. */
class BillingApi implements AutoCloseable {
    static final int MAX_ATTEMPTS = 4;
    static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(30);
    static final Duration READ_TIMEOUT = Duration.ofSeconds(120);
    private static final Duration MAX_RETRY_DELAY = Duration.ofSeconds(60);
    private static final Duration BASE_RETRY_DELAY = Duration.ofSeconds(2);

    private final RunContext runContext;
    private final String token;
    private final HttpClient client;

    BillingApi(RunContext runContext, String token) throws Exception {
        this.runContext = runContext;
        this.token = token;
        this.client = HttpClient.builder()
            .runContext(runContext)
            .configuration(
                HttpConfiguration.builder()
                    .timeout(
                        TimeoutConfiguration.builder()
                            .connectTimeout(Property.ofValue(CONNECT_TIMEOUT))
                            .readIdleTimeout(Property.ofValue(READ_TIMEOUT))
                            .build()
                    )
                    .build()
            )
            .build();
    }

    /** Sends one request and returns the JSON body; {@code body} is null for a GET. Retries 408, 429 and 5xx. */
    @SuppressWarnings("unchecked")
    Map<String, Object> send(String method, String url, Map<String, Object> body) throws Exception {
        for (int attempt = 1;; attempt++) {
            var request = HttpRequest.builder()
                .uri(URI.create(url))
                .method(method)
                .addHeader("Authorization", "Bearer " + token)
                .addHeader("Content-Type", "application/json");
            if (body != null) {
                request.body(HttpRequest.JsonRequestBody.builder().content(body).build());
            }
            try {
                Map<String, Object> response = client.request(request.build(), Map.class).getBody();
                return response == null ? Map.of() : response;
            } catch (HttpClientResponseException e) {
                int status = e.getResponse() == null ? -1 : e.getResponse().getStatus().getCode();
                if (!isRetryable(status) || attempt >= MAX_ATTEMPTS) {
                    throw new IllegalStateException(
                        "Azure request " + method + " " + url + " failed with HTTP " + (status < 0 ? "unknown" : status) + ": " + e.getMessage(), e
                    );
                }
                var delay = retryDelay(e.getResponse(), attempt);
                runContext.logger().warn("Azure returned HTTP {} for {} {}, retrying in {} ms (attempt {}/{})", status, method, url, delay.toMillis(), attempt, MAX_ATTEMPTS);
                sleep(delay);
            }
        }
    }

    static boolean isRetryable(int status) {
        return status == 408 || status == 429 || status >= 500;
    }

    /** Honors a {@code Retry-After} header in seconds, otherwise backs off exponentially; capped at one minute. */
    static Duration retryDelay(HttpResponse<?> response, int attempt) {
        if (response != null && response.getHeaders() != null) {
            var retryAfter = response.getHeaders().firstValue("Retry-After");
            if (retryAfter.isPresent()) {
                try {
                    var seconds = Duration.ofSeconds(Math.max(0, Long.parseLong(retryAfter.get().trim())));
                    return seconds.compareTo(MAX_RETRY_DELAY) > 0 ? MAX_RETRY_DELAY : seconds;
                } catch (NumberFormatException ignored) {
                    // HTTP-date form: fall back to the exponential backoff
                }
            }
        }
        var backoff = BASE_RETRY_DELAY.multipliedBy(1L << (attempt - 1));
        return backoff.compareTo(MAX_RETRY_DELAY) > 0 ? MAX_RETRY_DELAY : backoff;
    }

    void sleep(Duration delay) throws InterruptedException {
        Thread.sleep(delay.toMillis());
    }

    @Override
    public void close() throws IOException {
        client.close();
    }
}
