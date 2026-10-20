package io.kestra.plugin.azure.devops.pipelines;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.scheduler.model.TriggerState;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class PipelinesTest {
    private static HttpServer server;
    private static String baseUrl;
    private static final AtomicBoolean cancelCalled = new AtomicBoolean(false);
    private static final AtomicInteger runCreateCount = new AtomicInteger(0);

    @Inject
    private RunContextFactory runContextFactory;

    @BeforeAll
    static void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        int port = server.getAddress().getPort();
        baseUrl = "http://127.0.0.1:" + port;

        server.createContext("/my-org/my-project/_apis", exchange ->
        {
            String path = exchange.getRequestURI().getPath();
            String method = exchange.getRequestMethod();
            String auth = exchange.getRequestHeaders().getFirst("Authorization");

            if (auth == null || !auth.startsWith("Basic ")) {
                sendResponse(exchange, 401, "{\"message\":\"Unauthorized\"}");
                return;
            }

            if ("GET".equals(method) && path.endsWith("/pipelines/42/runs/101")) {
                sendResponse(exchange, 200, """
                    {
                      "id": 101,
                      "name": "20261020.1",
                      "state": "completed",
                      "result": "succeeded",
                      "createdDate": "2026-10-20T00:00:00Z",
                      "finishedDate": "2026-10-20T00:05:00Z",
                      "url": "%s/my-org/my-project/_apis/pipelines/42/runs/101",
                      "_links": {
                        "web": {
                          "href": "https://dev.azure.com/my-org/my-project/_build/results?buildId=101"
                        }
                      }
                    }
                    """.formatted(baseUrl));
                return;
            }

            if ("GET".equals(method) && path.endsWith("/pipelines/42/runs")) {
                sendResponse(exchange, 200, """
                    {
                      "count": 2,
                      "value": [
                        {
                          "id": 101,
                          "name": "20261020.1",
                          "state": "completed",
                          "result": "succeeded",
                          "createdDate": "2026-10-20T00:00:00Z",
                          "finishedDate": "2026-10-20T00:05:00Z",
                          "_links": {
                            "web": {
                              "href": "https://dev.azure.com/my-org/my-project/_build/results?buildId=101"
                            }
                          }
                        },
                        {
                          "id": 102,
                          "name": "20261020.2",
                          "state": "inProgress",
                          "createdDate": "2026-10-20T00:10:00Z"
                        }
                      ]
                    }
                    """);
                return;
            }

            if ("POST".equals(method) && path.endsWith("/pipelines/42/runs")) {
                runCreateCount.incrementAndGet();
                sendResponse(exchange, 200, """
                    {
                      "id": 103,
                      "name": "20261020.3",
                      "state": "completed",
                      "result": "succeeded",
                      "createdDate": "2026-10-20T00:20:00Z",
                      "finishedDate": "2026-10-20T00:22:00Z",
                      "_links": {
                        "web": {
                          "href": "https://dev.azure.com/my-org/my-project/_build/results?buildId=103"
                        }
                      }
                    }
                    """);
                return;
            }

            if ("PATCH".equals(method) && path.contains("/build/builds/")) {
                cancelCalled.set(true);
                sendResponse(exchange, 200, """
                    {
                      "id": 101,
                      "status": "cancelling"
                    }
                    """);
                return;
            }

            if ("GET".equals(method) && path.endsWith("/pipelines/42")) {
                sendResponse(exchange, 200, """
                    {
                      "id": 42,
                      "name": "MyPipeline",
                      "folder": "\\\\",
                      "revision": 2,
                      "url": "%s/my-org/my-project/_apis/pipelines/42",
                      "_links": {
                        "web": {
                          "href": "https://dev.azure.com/my-org/my-project/_build/definitions?definitionId=42"
                        }
                      }
                    }
                    """.formatted(baseUrl));
                return;
            }

            if ("GET".equals(method) && path.endsWith("/pipelines")) {
                sendResponse(exchange, 200, """
                    {
                      "count": 1,
                      "value": [
                        {
                          "id": 42,
                          "name": "MyPipeline",
                          "folder": "\\\\",
                          "revision": 2,
                          "_links": {
                            "web": {
                              "href": "https://dev.azure.com/my-org/my-project/_build/definitions?definitionId=42"
                            }
                          }
                        }
                      ]
                    }
                    """);
                return;
            }

            if ("POST".equals(method) && path.endsWith("/pipelines")) {
                sendResponse(exchange, 200, """
                    {
                      "id": 43,
                      "name": "MyNewPipeline",
                      "folder": "\\\\",
                      "revision": 1,
                      "_links": {
                        "web": {
                          "href": "https://dev.azure.com/my-org/my-project/_build/definitions?definitionId=43"
                        }
                      }
                    }
                    """);
                return;
            }

            sendResponse(exchange, 404, "{\"message\":\"Not Found\"}");
        });

        server.start();
    }

    @AfterAll
    static void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    private static void sendResponse(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    @Test
    void testGetRun() throws Exception {
        GetRun task = GetRun.builder()
            .organization(Property.ofValue("my-org"))
            .project(Property.ofValue("my-project"))
            .baseUrl(Property.ofValue(baseUrl))
            .personalAccessToken(Property.ofValue("test-pat-token"))
            .pipelineId(Property.ofValue(42))
            .runId(Property.ofValue(101))
            .build();

        RunContext runContext = runContextFactory.of();
        GetRun.Output output = task.run(runContext);

        assertThat(output.getId(), is(101));
        assertThat(output.getName(), is("20261020.1"));
        assertThat(output.getState(), is("completed"));
        assertThat(output.getResult(), is("succeeded"));
        assertThat(output.getWebUrl(), containsString("buildId=101"));
        assertThat(output.getRun(), notNullValue());
    }

    @Test
    void testListRuns() throws Exception {
        ListRuns task = ListRuns.builder()
            .organization(Property.ofValue("my-org"))
            .project(Property.ofValue("my-project"))
            .baseUrl(Property.ofValue(baseUrl))
            .personalAccessToken(Property.ofValue("test-pat-token"))
            .pipelineId(Property.ofValue(42))
            .state(Property.ofValue("completed"))
            .build();

        RunContext runContext = runContextFactory.of();
        ListRuns.Output output = task.run(runContext);

        assertThat(output.getTotal(), is(1));
        assertThat(output.getRuns().get(0).getId(), is(101));
        assertThat(output.getRuns().get(0).getState(), is("completed"));
    }

    @Test
    void testRunPipeline() throws Exception {
        Run task = Run.builder()
            .organization(Property.ofValue("my-org"))
            .project(Property.ofValue("my-project"))
            .baseUrl(Property.ofValue(baseUrl))
            .personalAccessToken(Property.ofValue("test-pat-token"))
            .pipelineId(Property.ofValue(42))
            .branch(Property.ofValue("main"))
            .templateParameters(Property.ofValue(Map.of("env", "test")))
            .variables(Property.ofValue(Map.of("DEBUG", "true")))
            .wait(Property.ofValue(false))
            .build();

        RunContext runContext = runContextFactory.of();
        Run.Output output = task.run(runContext);

        assertThat(output.getId(), is(103));
        assertThat(output.getState(), is("completed"));
        assertThat(output.getResult(), is("succeeded"));
    }

    @Test
    void testCancel() throws Exception {
        cancelCalled.set(false);

        Cancel task = Cancel.builder()
            .organization(Property.ofValue("my-org"))
            .project(Property.ofValue("my-project"))
            .baseUrl(Property.ofValue(baseUrl))
            .personalAccessToken(Property.ofValue("test-pat-token"))
            .runId(Property.ofValue(101))
            .build();

        RunContext runContext = runContextFactory.of();
        Cancel.Output output = task.run(runContext);

        assertThat(output.getRunId(), is(101));
        assertThat(output.getStatus(), is("Cancelling"));
        assertThat(cancelCalled.get(), is(true));
    }

    @Test
    void testTriggerEvaluationWithKVWatermark() throws Exception {
        Trigger trigger = Trigger.builder()
            .id("trg-" + IdUtils.create())
            .type(Trigger.class.getName())
            .organization(Property.ofValue("my-org"))
            .project(Property.ofValue("my-project"))
            .baseUrl(Property.ofValue(baseUrl))
            .personalAccessToken(Property.ofValue("test-pat-token"))
            .pipelineId(Property.ofValue(42))
            .interval(Duration.ofSeconds(60))
            .build();

        Map.Entry<ConditionContext, TriggerState> entry = TestsUtils.mockTrigger(runContextFactory, trigger);
        ConditionContext conditionContext = entry.getKey();
        io.kestra.core.models.triggers.TriggerContext triggerContext = entry.getValue().context();

        // First poll: observes run 101 in completed status -> generates Execution
        Optional<Execution> first = trigger.evaluate(conditionContext, triggerContext);
        assertThat(first.isPresent(), is(true));
        assertThat(first.get().getTrigger().getVariables().get("id"), is(101));
        assertThat(first.get().getTrigger().getVariables().get("result"), is("succeeded"));

        // Second poll: same run 101 -> must NOT refire (StatefulTriggerService deduplication)
        Optional<Execution> second = trigger.evaluate(conditionContext, triggerContext);
        assertThat(second.isPresent(), is(false));
    }

    @Test
    void testMissingCredentialsThrows() {
        GetRun task = GetRun.builder()
            .organization(Property.ofValue("my-org"))
            .project(Property.ofValue("my-project"))
            .baseUrl(Property.ofValue(baseUrl))
            .pipelineId(Property.ofValue(42))
            .runId(Property.ofValue(101))
            .build();

        RunContext runContext = runContextFactory.of();
        assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
    }

    @Test
    void testGetPipeline() throws Exception {
        Get task = Get.builder()
            .organization(Property.ofValue("my-org"))
            .project(Property.ofValue("my-project"))
            .baseUrl(Property.ofValue(baseUrl))
            .personalAccessToken(Property.ofValue("test-pat-token"))
            .pipelineId(Property.ofValue(42))
            .build();

        RunContext runContext = runContextFactory.of();
        Get.Output output = task.run(runContext);

        assertThat(output.getId(), is(42));
        assertThat(output.getName(), is("MyPipeline"));
        assertThat(output.getFolder(), is("\\"));
        assertThat(output.getRevision(), is(2));
        assertThat(output.getWebUrl(), containsString("definitionId=42"));
        assertThat(output.getPipeline(), notNullValue());
    }

    @Test
    void testListPipelines() throws Exception {
        List task = List.builder()
            .organization(Property.ofValue("my-org"))
            .project(Property.ofValue("my-project"))
            .baseUrl(Property.ofValue(baseUrl))
            .personalAccessToken(Property.ofValue("test-pat-token"))
            .build();

        RunContext runContext = runContextFactory.of();
        List.Output output = task.run(runContext);

        assertThat(output.getTotal(), is(1));
        assertThat(output.getPipelines().get(0).getId(), is(42));
        assertThat(output.getPipelines().get(0).getName(), is("MyPipeline"));
    }

    @Test
    void testCreatePipeline() throws Exception {
        Create task = Create.builder()
            .organization(Property.ofValue("my-org"))
            .project(Property.ofValue("my-project"))
            .baseUrl(Property.ofValue(baseUrl))
            .personalAccessToken(Property.ofValue("test-pat-token"))
            .name(Property.ofValue("MyNewPipeline"))
            .folder(Property.ofValue("\\"))
            .configuration(Property.ofValue(Map.of("type", "yaml", "path", "azure-pipelines.yml")))
            .build();

        RunContext runContext = runContextFactory.of();
        Create.Output output = task.run(runContext);

        assertThat(output.getId(), is(43));
        assertThat(output.getName(), is("MyNewPipeline"));
        assertThat(output.getFolder(), is("\\"));
        assertThat(output.getRevision(), is(1));
        assertThat(output.getWebUrl(), containsString("definitionId=43"));
        assertThat(output.getPipeline(), notNullValue());
    }
}
