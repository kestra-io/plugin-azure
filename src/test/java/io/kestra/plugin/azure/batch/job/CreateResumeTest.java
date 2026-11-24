package io.kestra.plugin.azure.batch.job;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.microsoft.azure.batch.protocol.models.PoolInformation;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.runners.ScriptService;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.azure.shared.batch.models.Job;
import io.kestra.plugin.azure.shared.batch.models.Task;

import jakarta.inject.Inject;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class CreateResumeTest {
    private static final String RESUMED_OUTPUT = "::{\"outputs\":{\"resumed\":true}}::\n";

    @Inject
    private RunContextFactory runContextFactory;

    private WireMockServer server;
    private String taskId;
    private String executionId;
    private String taskRunId;

    @BeforeEach
    void start() {
        server = new WireMockServer(options().dynamicPort());
        server.start();
        // job lookups that match no stubbed job return an empty list
        server.stubFor(get(urlPathEqualTo("/jobs")).atPriority(10).willReturn(okJson("{\"value\": []}")));
        taskId = IdUtils.create();
        executionId = IdUtils.create();
        taskRunId = IdUtils.create();
    }

    @AfterEach
    void stop() {
        server.stop();
    }

    @Test
    void resumesTheJobCreatedWithTheUserJobId() throws Exception {
        var create = create("myjob");
        stubJobs("myjob", job("myjob", "active", create, runContext(executionId, 1)));
        stubTask("myjob", "completed", false, RESUMED_OUTPUT);

        var output = create.run(runContext(executionId, 2));

        assertEquals(true, output.getVars().get("resumed"));
        server.verify(0, postRequestedFor(urlPathEqualTo("/jobs")));
    }

    @Test
    void resumesTheJobCreatedWithATemplatedJobId() throws Exception {
        var create = create("job-{{ execution.id }}");
        var jobId = "job-" + executionId;
        stubJobs(jobId, job(jobId, "active", create, runContext(executionId, 1)));
        stubTask(jobId, "completed", false, RESUMED_OUTPUT);

        var output = create.run(runContext(executionId, 2));

        assertEquals(true, output.getVars().get("resumed"));
        server.verify(0, postRequestedFor(urlPathEqualTo("/jobs")));
    }

    @Test
    void resumesATaskRunnerJobWithAnotherSuffix() throws Exception {
        var runContext = runContext(executionId, 2);
        var jobName = ScriptService.jobName(runContext);
        var prefix = jobName.substring(0, jobName.lastIndexOf('-') + 1);
        var create = create(prefix + "newsuffix");
        stubJobs(prefix, job(prefix + "oldsuffix", "active", create, runContext(executionId, 1)));
        stubTask(prefix + "oldsuffix", "completed", false, RESUMED_OUTPUT);

        var output = create.run(runContext);

        assertEquals(true, output.getVars().get("resumed"));
        server.verify(0, postRequestedFor(urlPathEqualTo("/jobs")));
    }

    @Test
    void resumesAnActiveJobWithARunningTask() throws Exception {
        var create = create("myjob");
        stubJobs("myjob", job("myjob", "active", create, runContext(executionId, 1)));
        // the task is still running when the job is resumed, then completes
        server.stubFor(get(urlPathEqualTo("/jobs/myjob/tasks")).inScenario("task").whenScenarioStateIs("Started")
            .willReturn(okJson(tasks("running", false))).willSetStateTo("completed"));
        server.stubFor(get(urlPathEqualTo("/jobs/myjob/tasks")).inScenario("task").whenScenarioStateIs("completed")
            .willReturn(okJson(tasks("completed", false))));
        stubFiles("myjob", RESUMED_OUTPUT);

        var output = create.run(runContext(executionId, 2));

        assertEquals(true, output.getVars().get("resumed"));
        server.verify(0, postRequestedFor(urlPathEqualTo("/jobs")));
    }

    @Test
    void doesNotResumeAJobOfAnotherExecution() throws Exception {
        var create = create("myjob");
        stubJobs("myjob", job("myjob", "active", create, runContext(IdUtils.create(), 1)));
        stubTask("myjob", "completed", false, RESUMED_OUTPUT);

        assertCreatesANewJob(create);
    }

    @Test
    void doesNotResumeACompletedJob() throws Exception {
        var create = create("myjob");
        stubJobs("myjob", job("myjob", "completed", create, runContext(executionId, 1)));
        stubTask("myjob", "completed", false, RESUMED_OUTPUT);

        assertCreatesANewJob(create);
    }

    @Test
    void doesNotResumeAJobBeingDeleted() throws Exception {
        var create = create("myjob");
        stubJobs("myjob", job("myjob", "deleting", create, runContext(executionId, 1)));
        stubTask("myjob", "completed", false, RESUMED_OUTPUT);

        assertCreatesANewJob(create);
    }

    @Test
    void doesNotResumeAnActiveJobWhoseTasksFailed() throws Exception {
        var create = create("myjob");
        stubJobs("myjob", job("myjob", "active", create, runContext(executionId, 1)));
        stubTask("myjob", "completed", true, "");

        assertCreatesANewJob(create);
    }

    private void assertCreatesANewJob(Create create) {
        // job creation is not stubbed, so creating a new job fails
        assertThrows(Exception.class, () -> create.run(runContext(executionId, 2)));

        server.verify(1, postRequestedFor(urlPathEqualTo("/jobs")));
    }

    private Create create(String jobId) {
        return Create.builder()
            .id(taskId)
            .type(Create.class.getName())
            .endpoint(Property.ofValue(server.baseUrl()))
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

    private RunContext runContext(String executionId, int attemptsCount) {
        return runContextFactory.of(Map.of(
            "flow", Map.of("namespace", "company.team", "id", "my-flow"),
            "task", Map.of("id", taskId),
            "execution", Map.of("id", executionId),
            "taskrun", Map.of("id", taskRunId, "attemptsCount", attemptsCount)
        ));
    }

    private void stubJobs(String idPrefix, Map<String, Object> job) throws Exception {
        server.stubFor(get(urlPathEqualTo("/jobs"))
            .withQueryParam("$filter", equalTo("startswith(id, '" + idPrefix + "')"))
            .willReturn(okJson(JacksonMapper.ofJson().writeValueAsString(Map.of("value", List.of(job))))));
    }

    private void stubTask(String jobId, String state, boolean failed, String stdout) {
        server.stubFor(get(urlPathEqualTo("/jobs/" + jobId + "/tasks")).willReturn(okJson(tasks(state, failed))));
        stubFiles(jobId, stdout);
    }

    private void stubFiles(String jobId, String stdout) {
        server.stubFor(get(urlPathEqualTo("/jobs/" + jobId + "/tasks/echo/files/stdout.txt"))
            .willReturn(ok(stdout).withHeader("Content-Type", "application/octet-stream")));
        server.stubFor(get(urlPathEqualTo("/jobs/" + jobId + "/tasks/echo/files/stderr.txt"))
            .willReturn(ok("").withHeader("Content-Type", "application/octet-stream")));
    }

    private static String tasks(String state, boolean failed) {
        var failureInfo = failed ? ", \"failureInfo\": {\"category\": \"usererror\", \"code\": \"FailureExitCode\"}" : "";
        return """
            {"value": [{"id": "echo", "state": "%s",
              "executionInfo": {"startTime": "2026-01-01T00:00:00Z", "endTime": "2026-01-01T00:00:02Z",
                                "exitCode": %d, "retryCount": 0, "requeueCount": 0%s}}]}
            """.formatted(state, failed ? 1 : 0, failureInfo);
    }

    private static Map<String, Object> job(String id, String state, Create create, RunContext firstAttempt) throws Exception {
        var created = create.getJob().to(firstAttempt, new PoolInformation().withPoolId("pool"));
        return Map.of(
            "id", id,
            "state", state,
            "creationTime", "2026-01-01T00:00:00Z",
            "metadata", created.metadata().stream().map(item -> Map.of("name", item.name(), "value", item.value())).toList()
        );
    }
}
