package io.kestra.plugin.azure.batch.job;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.Test;

import com.microsoft.azure.batch.protocol.models.PoolInformation;
import com.sun.net.httpserver.HttpServer;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.runners.ScriptService;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.azure.shared.batch.models.Job;
import io.kestra.plugin.azure.shared.batch.models.Task;

import jakarta.inject.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@KestraTest
class CreateResumeTest {
    @Inject
    RunContextFactory runContextFactory;

    @Test
    void resumesTheJobCreatedWithTheUserJobId() throws Exception {
        try (var endpoint = new FakeBatch()) {
            var runContext = runContext(2);
            var firstAttempt = runContext(1);
            var create = create(endpoint, "myjob");
            endpoint.jobs("startswith(id, 'myjob')", job("myjob", create, firstAttempt));
            endpoint.completedTask("myjob", "echo", "::{\"outputs\":{\"resumed\":true}}::\n");

            var output = create.run(runContext);

            assertEquals(true, output.getVars().get("resumed"));
            assertTrue(endpoint.requests("POST", "/jobs").isEmpty(), "no new job is created");
        }
    }

    @Test
    void resumesTheJobCreatedWithATemplatedJobId() throws Exception {
        try (var endpoint = new FakeBatch()) {
            var runContext = runContext(2);
            var firstAttempt = runContext(1);
            var create = create(endpoint, "job-{{ execution.id }}");
            endpoint.jobs("startswith(id, 'job-exec')", job("job-exec", create, firstAttempt));
            endpoint.completedTask("job-exec", "echo", "::{\"outputs\":{\"resumed\":true}}::\n");

            var output = create.run(runContext);

            assertEquals(true, output.getVars().get("resumed"));
            assertTrue(endpoint.requests("POST", "/jobs").isEmpty(), "no new job is created");
        }
    }

    @Test
    void doesNotResumeAJobOfAnotherExecution() throws Exception {
        try (var endpoint = new FakeBatch()) {
            var runContext = runContext(2);
            var otherExecution = runContext("other-exec", 1);
            var create = create(endpoint, "myjob");
            endpoint.jobs("startswith(id, 'myjob')", job("myjob", create, otherExecution));
            endpoint.completedTask("myjob", "echo", "::{\"outputs\":{\"resumed\":true}}::\n");

            // the fake endpoint has no reply for job creation, so creating a new job fails
            assertThrows(Exception.class, () -> create.run(runContext));

            assertEquals(1, endpoint.requests("POST", "/jobs").size(), "a new job is created instead");
        }
    }

    @Test
    void resumesATaskRunnerJobWithAnotherSuffix() throws Exception {
        try (var endpoint = new FakeBatch()) {
            var runContext = runContext(2);
            var firstAttempt = runContext(1);
            var jobName = ScriptService.jobName(runContext);
            var prefix = jobName.substring(0, jobName.lastIndexOf('-') + 1);
            var create = create(endpoint, prefix + "newsuffix");
            endpoint.jobs("startswith(id, '" + prefix + "')", job(prefix + "oldsuffix", create, firstAttempt));
            endpoint.completedTask(prefix + "oldsuffix", "echo", "::{\"outputs\":{\"resumed\":true}}::\n");

            var output = create.run(runContext);

            assertEquals(true, output.getVars().get("resumed"));
            assertTrue(endpoint.requests("POST", "/jobs").isEmpty(), "no new job is created");
        }
    }

    private Create create(FakeBatch endpoint, String jobId) {
        return Create.builder()
            .id("create")
            .type(Create.class.getName())
            .endpoint(Property.ofValue(endpoint.url()))
            .account(Property.ofValue("test-account"))
            .accessKey(Property.ofValue("dGVzdC1rZXk="))
            .poolId(Property.ofValue("pool"))
            .job(Job.builder().id(jobId).build())
            .tasks(List.of(Task.builder().id("echo").commands(Property.ofValue(List.of("echo hello"))).build()))
            .completionCheckInterval(Property.ofValue(Duration.ofMillis(10)))
            .delete(Property.ofValue(false))
            .pushOutputFilesToInternalStorage(false)
            .build();
    }

    private RunContext runContext(int attemptsCount) {
        return runContext("exec", attemptsCount);
    }

    private RunContext runContext(String executionId, int attemptsCount) {
        return runContextFactory.of(
            Map.of(
                "flow", Map.of("namespace", "company.team", "id", "my-flow"),
                "task", Map.of("id", "create"),
                "execution", Map.of("id", executionId),
                "taskrun", Map.of("id", "taskrun", "attemptsCount", attemptsCount)
            )
        );
    }

    private static Map<String, Object> job(String id, Create create, RunContext firstAttempt) throws Exception {
        var created = create.getJob().to(firstAttempt, new PoolInformation().withPoolId("pool"));
        return Map.of(
            "id", id,
            "creationTime", "2026-01-01T00:00:00Z",
            "metadata", created.metadata().stream().map(item -> Map.of("name", item.name(), "value", item.value())).toList()
        );
    }

    private static final class FakeBatch implements AutoCloseable {
        private record Request(String method, String path, String query) {
        }

        private final HttpServer server;
        private final Map<String, String> replies = new ConcurrentHashMap<>();
        private final Map<String, String> jobLists = new ConcurrentHashMap<>();
        private final List<Request> requests = new CopyOnWriteArrayList<>();

        private FakeBatch() throws IOException {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/", exchange ->
            {
                var path = exchange.getRequestURI().getPath();
                var rawQuery = exchange.getRequestURI().getRawQuery();
                var query = rawQuery == null ? "" : URLDecoder.decode(rawQuery, StandardCharsets.UTF_8);
                requests.add(new Request(exchange.getRequestMethod(), path, query));

                String body;
                if (path.equals("/jobs") && exchange.getRequestMethod().equals("GET")) {
                    body = jobLists.entrySet().stream()
                        .filter(entry -> query.contains(entry.getKey()))
                        .map(Map.Entry::getValue)
                        .findFirst()
                        .orElse("{\"value\":[]}");
                } else {
                    body = replies.get(exchange.getRequestMethod() + " " + path);
                }

                if (body == null) {
                    var notFound = "{\"code\":\"NotFound\",\"message\":{\"lang\":\"en-US\",\"value\":\"Not found\"}}".getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(404, notFound.length);
                    try (var response = exchange.getResponseBody()) {
                        response.write(notFound);
                    }
                    return;
                }

                var bytes = body.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", path.contains("/files/") ? "application/octet-stream" : "application/json");
                exchange.sendResponseHeaders(200, bytes.length == 0 ? -1 : bytes.length);
                try (var response = exchange.getResponseBody()) {
                    response.write(bytes);
                }
            });
            server.start();
        }

        private void jobs(String filter, Map<String, Object> job) throws IOException {
            jobLists.put(filter, JacksonMapper.ofJson().writeValueAsString(Map.of("value", List.of(job))));
        }

        private void completedTask(String jobId, String taskId, String stdout) {
            replies.put("GET /jobs/" + jobId + "/tasks", """
                {"value": [{"id": "%s", "state": "completed",
                  "executionInfo": {"startTime": "2026-01-01T00:00:00Z", "endTime": "2026-01-01T00:00:02Z",
                                    "exitCode": 0, "retryCount": 0, "requeueCount": 0}}]}
                """.formatted(taskId));
            replies.put("GET /jobs/" + jobId + "/tasks/" + taskId + "/files/stdout.txt", stdout);
            replies.put("GET /jobs/" + jobId + "/tasks/" + taskId + "/files/stderr.txt", "");
        }

        private List<Request> requests(String method, String path) {
            return requests.stream().filter(r -> r.method().equals(method) && r.path().equals(path)).toList();
        }

        private String url() {
            return "http://127.0.0.1:" + server.getAddress().getPort();
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }
}
