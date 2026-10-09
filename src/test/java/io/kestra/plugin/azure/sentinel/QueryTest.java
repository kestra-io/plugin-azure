package io.kestra.plugin.azure.sentinel;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.azure.core.http.HttpClient;
import com.azure.monitor.query.logs.LogsQueryClientBuilder;
import com.github.tomakehurst.wiremock.WireMockServer;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;

import jakarta.inject.Inject;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@KestraTest
class QueryTest {
    @Inject
    RunContextFactory runContextFactory;

    private WireMockServer server;
    private LogsQueryClientBuilder clientBuilder;

    @BeforeEach
    void startServer() {
        server = new WireMockServer(wireMockConfig().dynamicPort());
        server.start();
        // Exercise the production SDK factory and policies; redirect only the transport to WireMock.
        HttpClient transport = HttpClient.createDefault();
        clientBuilder = new LogsQueryClientBuilder()
            .credential(
                request -> reactor.core.publisher.Mono.just(
                    new com.azure.core.credential.AccessToken(
                        "test-token", java.time.OffsetDateTime.now().plusHours(1)
                    )
                )
            )
            .httpClient(request -> transport.send(request.copy().setUrl(server.baseUrl() + request.getUrl().getFile())));
    }

    @AfterEach
    void stopServer() {
        server.stop();
    }

    private Query task(FetchType mode) throws Exception {
        Query task = spy(
            Query.builder()
                .id("query")
                .type(Query.class.getName())
                .workspaceId(Property.ofExpression("{{ inputs.workspace }}"))
                .query(Property.ofExpression("{{ inputs.kql }}"))
                .fetchType(Property.ofValue(mode))
                .build()
        );
        doReturn(clientBuilder).when(task).clientBuilder(any());
        return task;
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
    void sdkUsesLogAnalyticsScope() throws Exception {
        response(table("[]"));
        List<String> scopes = new ArrayList<>();
        HttpClient transport = HttpClient.createDefault();
        clientBuilder = new LogsQueryClientBuilder()
            .credential(request ->
            {
                scopes.addAll(request.getScopes());
                return reactor.core.publisher.Mono.just(
                    new com.azure.core.credential.AccessToken(
                        "test-token", java.time.OffsetDateTime.now().plusHours(1)
                    )
                );
            })
            .httpClient(request -> transport.send(request.copy().setUrl(server.baseUrl() + request.getUrl().getFile())));
        assertEquals(0L, task(FetchType.FETCH).run(context()).getCount());
        assertEquals(List.of("https://api.loganalytics.io/.default"), scopes);
        server.verify(
            postRequestedFor(urlEqualTo("/v1/workspaces/workspace-guid/query"))
                .withHeader("Authorization", equalTo("Bearer test-token"))
        );
    }

    @Test
    void fetchRendersPropertiesAndNormalizesSdkTypes() throws Exception {
        response(table(rows()));
        Query.Output output = task(FetchType.FETCH).run(context());
        assertEquals(2L, output.getCount());
        assertEquals("long", output.getColumns().get(1).getType());
        assertEquals("count", output.getColumns().get(1).getName());
        Map<String, Object> row = output.getRows().getFirst();
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
        Query.Output first = task(FetchType.FETCH_ONE).run(context());
        assertEquals(1L, first.getCount());
        assertEquals("alert", first.getRow().get("title"));
        assertNull(first.getRows());
        response(table("[]"));
        assertEquals(0L, task(FetchType.FETCH_ONE).run(context()).getCount());
        assertNull(task(FetchType.FETCH_ONE).run(context()).getRow());
        Query.Output empty = task(FetchType.FETCH).run(context());
        assertEquals(List.of(), empty.getRows());
        assertEquals(8, empty.getColumns().size());
        assertEquals("long", empty.getColumns().get(1).getType());
    }

    @Test
    void storeRoundTripsThroughInternalStorage() throws Exception {
        response(table(rows()));
        RunContext context = context();
        Query.Output output = task(FetchType.STORE).run(context);
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
        Query.Output empty = task(FetchType.STORE).run(context);
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
        IllegalStateException error = assertThrows(IllegalStateException.class, () -> task(FetchType.STORE).run(context()));
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
    void validatesBeforeSendingRequest() throws Exception {
        assertEquals(FetchType.STORE, context().render(Query.builder().build().getFetchType()).as(FetchType.class).orElseThrow());
        for (Duration span : List.of(Duration.ZERO, Duration.ofSeconds(-1))) {
            Query task = spy(
                Query.builder().workspaceId(Property.ofValue("workspace"))
                    .query(Property.ofValue("query")).timespan(Property.ofValue(span)).build()
            );
            assertThrows(IllegalArgumentException.class, () -> task.run(context()));
            verify(task, never()).queryClient(any());
        }
        for (Duration timeout : List.of(Duration.ZERO, Duration.ofSeconds(-1), Duration.ofMinutes(11))) {
            Query task = spy(
                Query.builder().workspaceId(Property.ofValue("workspace"))
                    .query(Property.ofValue("query")).timeout(Property.ofValue(timeout)).build()
            );
            assertThrows(IllegalArgumentException.class, () -> task.run(context()));
            verify(task, never()).queryClient(any());
        }
        assertThrows(IllegalArgumentException.class, () -> task(FetchType.NONE).run(context()));
        assertEquals(0, server.getAllServeEvents().size());
    }
}
