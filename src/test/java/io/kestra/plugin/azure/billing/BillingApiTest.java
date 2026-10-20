package io.kestra.plugin.azure.billing;

import java.net.http.HttpHeaders;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.stubbing.Scenario;

import io.kestra.core.http.HttpResponse;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class BillingApiTest {
    @Inject
    RunContextFactory runContextFactory;

    private WireMockServer server;
    private final List<Duration> sleeps = new ArrayList<>();

    @BeforeEach
    void start() {
        server = new WireMockServer(options().dynamicPort());
        server.start();
    }

    @AfterEach
    void stop() {
        server.stop();
    }

    private BillingApi api() throws Exception {
        return new BillingApi(runContextFactory.of(), "test-token") {
            @Override
            void sleep(Duration delay) {
                sleeps.add(delay);
            }
        };
    }

    @Test
    void givenOk_whenSend_thenSendsBearerAndBody() throws Exception {
        server.stubFor(post("/query").willReturn(okJson("{\"properties\":{\"rows\":[[1]]}}")));

        try (var api = api()) {
            var response = api.send("POST", server.baseUrl() + "/query", Map.of("type", "ActualCost"));
            assertThat(response, hasKey("properties"));
        }

        server.verify(
            postRequestedFor(urlEqualTo("/query"))
                .withHeader("Authorization", equalTo("Bearer test-token"))
                .withRequestBody(equalToJson("{\"type\":\"ActualCost\"}"))
        );
    }

    @Test
    void givenThrottled_whenSend_thenHonorsRetryAfterAndSucceeds() throws Exception {
        // the underlying HTTP client already retries a 429 once, so throttle three times to reach our own retry
        var states = List.of(Scenario.STARTED, "429-2", "429-3", "ok");
        for (int i = 0; i < 3; i++) {
            server.stubFor(
                get("/budgets").inScenario("throttle").whenScenarioStateIs(states.get(i))
                    .willReturn(aResponse().withStatus(429).withHeader("Retry-After", "1"))
                    .willSetStateTo(states.get(i + 1))
            );
        }
        server.stubFor(get("/budgets").inScenario("throttle").whenScenarioStateIs("ok").willReturn(okJson("{\"value\":[]}")));

        try (var api = api()) {
            assertThat(api.send("GET", server.baseUrl() + "/budgets", null), hasKey("value"));
        }

        assertThat(sleeps, contains(Duration.ofSeconds(1)));
        server.verify(4, getRequestedFor(urlEqualTo("/budgets")).withHeader("Authorization", equalTo("Bearer test-token")));
    }

    @Test
    void givenClientError_whenSend_thenFailsWithoutRetry() throws Exception {
        server.stubFor(get("/budgets/missing").willReturn(aResponse().withStatus(404).withBody("{\"error\":{\"code\":\"NotFound\"}}")));

        try (var api = api()) {
            var e = assertThrows(IllegalStateException.class, () -> api.send("GET", server.baseUrl() + "/budgets/missing", null));
            assertThat(e.getMessage(), containsString("HTTP 404"));
        }

        assertThat(sleeps, empty());
        server.verify(1, getRequestedFor(urlEqualTo("/budgets/missing")));
    }

    @Test
    void givenServerErrorEveryTime_whenSend_thenStopsAfterMaxAttempts() throws Exception {
        server.stubFor(get("/budgets").willReturn(aResponse().withStatus(500)));

        try (var api = api()) {
            assertThrows(IllegalStateException.class, () -> api.send("GET", server.baseUrl() + "/budgets", null));
        }

        server.verify(BillingApi.MAX_ATTEMPTS, getRequestedFor(urlEqualTo("/budgets")));
        assertThat(sleeps, contains(Duration.ofSeconds(2), Duration.ofSeconds(4), Duration.ofSeconds(8)));
    }

    @Test
    void givenNegativeRetryAfter_whenRetryDelay_thenZero() {
        var response = HttpResponse.builder()
            .status(HttpResponse.Status.builder().code(429).build())
            .headers(HttpHeaders.of(Map.of("Retry-After", List.of("-5")), (k, v) -> true))
            .build();

        assertThat(BillingApi.retryDelay(response, 1), is(Duration.ZERO));
    }
}
