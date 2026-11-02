package io.kestra.plugin.azure.sentinel;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.github.tomakehurst.wiremock.WireMockServer;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.utils.IdUtils;

import jakarta.inject.Inject;
import reactor.core.publisher.Mono;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@KestraTest
class QueryTest {
    @Inject
    RunContextFactory runContextFactory;

    private static final String[] TRUST_PROPERTIES = { "javax.net.ssl.trustStore", "javax.net.ssl.trustStorePassword", "javax.net.ssl.trustStoreType" };

    private WireMockServer server;
    private final List<String> scopes = new ArrayList<>();
    private final Map<String, String> previousTrust = new HashMap<>();

    // The Azure SDK refuses bearer tokens over plain HTTP, so WireMock serves a self-signed localhost certificate that the JVM trusts for the test.
    @BeforeEach
    void startServer() throws URISyntaxException {
        var keystore = Path.of(QueryTest.class.getResource("/sentinel/localhost.p12").toURI()).toString();
        server = new WireMockServer(
            wireMockConfig().dynamicPort().dynamicHttpsPort()
                .keystorePath(keystore).keystorePassword("changeit").keyManagerPassword("changeit").keystoreType("PKCS12")
        );
        server.start();
        for (var property : TRUST_PROPERTIES) {
            previousTrust.put(property, System.getProperty(property));
        }
        System.setProperty("javax.net.ssl.trustStore", keystore);
        System.setProperty("javax.net.ssl.trustStorePassword", "changeit");
        System.setProperty("javax.net.ssl.trustStoreType", "PKCS12");
    }

    @AfterEach
    void stopServer() {
        server.stop();
        previousTrust.forEach((property, value) ->
        {
            if (value == null) {
                System.clearProperty(property);
            } else {
                System.setProperty(property, value);
            }
        });
    }

    private String endpoint() {
        return "https://localhost:" + server.httpsPort();
    }

    private Query connect(Query task) throws Exception {
        var spied = spy(task);
        TokenCredential credential = request ->
        {
            scopes.addAll(request.getScopes());
            return Mono.just(new AccessToken("test-token", OffsetDateTime.now().plusHours(1)));
        };
        doReturn(credential).when(spied).credentials(any());
        return spied;
    }

    private Query task(FetchType mode) throws Exception {
        return connect(
            Query.builder()
                .id(IdUtils.create())
                .type(Query.class.getName())
                .logAnalyticsEndpoint(Property.ofValue(endpoint()))
                .workspaceId(Property.ofExpression("{{ inputs.workspace }}"))
                .query(Property.ofExpression("{{ inputs.kql }}"))
                .fetchType(Property.ofValue(mode))
                .build()
        );
    }

    private RunContext context() {
        return runContextFactory.of(Map.of("inputs", Map.of("workspace", "workspace-guid", "kql", "SecurityAlert | take 2")));
    }

    private void response(String body) {
        server.stubFor(
            post(urlEqualTo("/v1/workspaces/workspace-guid/query"))
                .willReturn(okJson(body))
        );
    }

    private static String table(String rows) {
        return """
            {"tables":[{"name":"PrimaryResult","columns":[
              {"name":"title","type":"string"},
              {"name":"count","type":"long"},
              {"name":"enabled","type":"bool"},
              {"name":"details","type":"dynamic"},
              {"name":"timestamp","type":"datetime"},
              {"name":"nullable","type":"int"},
              {"name":"ratio","type":"real"},
              {"name":"number","type":"int"}
            ],"rows":%s}]}
            """.formatted(rows);
    }

    private static String rows() {
        return """
            [["alert",9223372036854775806,true,"{\\"key\\":[1,2]}","2026-01-01T00:00:00Z",null,1.5,4],
             ["other",2,false,"null","2026-01-02T00:00:00Z",3,2.5,5]]
            """;
    }

    @Test
    void sendsBearerTokenToConfiguredEndpoint() throws Exception {
        response(table("[]"));
        assertEquals(0L, task(FetchType.FETCH).run(context()).getCount());
        assertEquals(1, scopes.size());
        server.verify(
            postRequestedFor(urlEqualTo("/v1/workspaces/workspace-guid/query"))
                .withHeader("Authorization", equalTo("Bearer test-token"))
        );
    }

    @Test
    void fetchRendersPropertiesAndNormalizesSdkTypes() throws Exception {
        response(table(rows()));
        var output = task(FetchType.FETCH).run(context());
        assertEquals(2L, output.getCount());
        assertEquals("long", output.getColumns().get(1).getType());
        assertEquals("count", output.getColumns().get(1).getName());
        var row = output.getRows().getFirst();
        assertEquals(9223372036854775806L, row.get("count"));
        assertEquals(true, row.get("enabled"));
        assertEquals(Map.of("key", List.of(1, 2)), row.get("details"));
        assertEquals("2026-01-01T00:00Z", row.get("timestamp"));
        assertNull(row.get("nullable"));
        assertEquals(1.5d, row.get("ratio"));
        assertEquals(4, row.get("number"));
        assertNull(output.getRows().get(1).get("details"));
        server.verify(
            postRequestedFor(urlEqualTo("/v1/workspaces/workspace-guid/query"))
                .withRequestBody(matchingJsonPath("$.query", equalTo("SecurityAlert | take 2")))
                .withRequestBody(matchingJsonPath("$.timespan", equalTo("PT24H")))
                .withHeader("Prefer", containing("wait=180"))
        );
    }

    @Test
    void fetchOneAndEmptyResults() throws Exception {
        response(table(rows()));
        var first = task(FetchType.FETCH_ONE).run(context());
        assertEquals(1L, first.getCount());
        assertEquals("alert", first.getRow().get("title"));
        assertNull(first.getRows());
        response(table("[]"));
        assertEquals(0L, task(FetchType.FETCH_ONE).run(context()).getCount());
        assertNull(task(FetchType.FETCH_ONE).run(context()).getRow());
        var empty = task(FetchType.FETCH).run(context());
        assertEquals(List.of(), empty.getRows());
        assertEquals(8, empty.getColumns().size());
        assertEquals("long", empty.getColumns().get(1).getType());
    }

    @Test
    void storeRoundTripsThroughInternalStorage() throws Exception {
        response(table(rows()));
        var context = context();
        var output = task(FetchType.STORE).run(context);
        List<Object> stored = new ArrayList<>();
        try (var input = context.storage().getFile(output.getUri())) {
            FileSerde.read(input, stored::add);
        }
        assertEquals(2L, output.getCount());
        assertEquals(2, stored.size());
        assertEquals("alert", ((Map<?, ?>) stored.getFirst()).get("title"));
        assertEquals(Map.of("key", List.of(1, 2)), ((Map<?, ?>) stored.getFirst()).get("details"));
        assertNull(output.getRows());
        response(table("[]"));
        var empty = task(FetchType.STORE).run(context);
        assertEquals(0L, empty.getCount());
        try (var input = context.storage().getFile(empty.getUri())) {
            assertEquals(-1, input.read());
        }
    }

    @Test
    void takesPrimaryTableAndHandlesNoTables() throws Exception {
        response("""
            {"tables":[
              {"name":"PrimaryResult","columns":[{"name":"value","type":"string"}],"rows":[["primary"]]},
              {"name":"Other","columns":[{"name":"value","type":"string"}],"rows":[["other"]]}
            ]}
            """);
        assertEquals("primary", task(FetchType.FETCH_ONE).run(context()).getRow().get("value"));
        response("{\"tables\":[]}");
        assertEquals(0L, task(FetchType.FETCH).run(context()).getCount());
    }

    @Test
    void rejectsPartialAndHttpErrors() throws Exception {
        response("""
            {"tables":[{"name":"PrimaryResult","columns":[],"rows":[]}],
             "error":{"code":"PartialError","message":"Query limit exceeded"}}
            """);
        var error = assertThrows(IllegalStateException.class, () -> task(FetchType.STORE).run(context()));
        assertTrue(error.getMessage().contains("PartialError"));
        server.stubFor(
            post(anyUrl()).willReturn(
                aResponse().withStatus(403)
                    .withHeader("Content-Type", "application/json")
                    .withBody("{\"error\":{\"code\":\"Forbidden\",\"message\":\"Access denied\"}}")
            )
        );
        assertThrows(Exception.class, () -> task(FetchType.FETCH).run(context()));
    }

    @Test
    void serverTimeoutIsIndependentOfTaskTimeout() throws Exception {
        response(table("[]"));
        var defaults = Query.builder().build();
        assertNull(defaults.getTimeout());
        assertEquals(Duration.ofMinutes(3), context().render(defaults.getServerTimeout()).as(Duration.class).orElseThrow());
        var task = connect(
            Query.builder()
                .id(IdUtils.create()).type(Query.class.getName())
                .logAnalyticsEndpoint(Property.ofValue(endpoint()))
                .workspaceId(Property.ofValue("workspace-guid")).query(Property.ofValue("print value = 1"))
                .timeout(Property.ofValue(Duration.ofMinutes(15)))
                .serverTimeout(Property.ofExpression("{{ inputs.serverTimeout }}"))
                .build()
        );
        var context = runContextFactory.of(Map.of("inputs", Map.of("serverTimeout", "PT1M")));
        assertEquals(0L, task.run(context).getCount());
        assertEquals(Duration.ofMinutes(15), context.render(task.getTimeout()).as(Duration.class).orElseThrow());
        server.verify(
            postRequestedFor(urlEqualTo("/v1/workspaces/workspace-guid/query"))
                .withHeader("Prefer", containing("wait=60"))
        );
    }

    @Test
    void validatesBeforeSendingRequest() throws Exception {
        assertEquals(FetchType.STORE, context().render(Query.builder().build().getFetchType()).as(FetchType.class).orElseThrow());
        for (var span : List.of(Duration.ZERO, Duration.ofSeconds(-1))) {
            var task = connect(
                Query.builder().workspaceId(Property.ofValue("workspace"))
                    .query(Property.ofValue("query")).timespan(Property.ofValue(span)).build()
            );
            assertThrows(IllegalArgumentException.class, () -> task.run(context()));
        }
        for (var timeout : List.of(Duration.ZERO, Duration.ofSeconds(-1), Duration.ofMinutes(11))) {
            var task = connect(
                Query.builder().workspaceId(Property.ofValue("workspace"))
                    .query(Property.ofValue("query")).serverTimeout(Property.ofValue(timeout)).build()
            );
            assertThrows(IllegalArgumentException.class, () -> task.run(context()));
        }
        assertThrows(IllegalArgumentException.class, () -> task(FetchType.NONE).run(context()));
        assertEquals(0, server.getAllServeEvents().size());
    }

    @Test
    void defaultsToPublicLogAnalyticsEndpoint() throws Exception {
        var defaults = Query.builder().build();
        assertEquals(Query.DEFAULT_LOG_ANALYTICS_ENDPOINT, context().render(defaults.getLogAnalyticsEndpoint()).as(String.class).orElseThrow());
        assertEquals("https://api.loganalytics.io", Query.DEFAULT_LOG_ANALYTICS_ENDPOINT);
    }
}
