package io.kestra.plugin.azure.devops.pipelines;

import java.time.Duration;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.stubbing.Scenario;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class PipelinesTest {
    private WireMockServer wireMock;
    private String baseUrl;

    @Inject
    private RunContextFactory runContextFactory;

    @BeforeEach
    void startServer() {
        wireMock = new WireMockServer(options().dynamicPort());
        wireMock.start();
        baseUrl = wireMock.baseUrl();
    }

    @AfterEach
    void stopServer() {
        if (wireMock != null) {
            wireMock.stop();
        }
    }

    @Test
    void testGetRun() throws Exception {
        wireMock.stubFor(
            get(urlPathEqualTo("/my-org/my-project/_apis/pipelines/42/runs/101"))
                .withHeader("Authorization", matching("Basic .*"))
                .willReturn(okJson("""
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
                    }
                    """))
        );

        var task = GetRun.builder()
            .organization(Property.ofValue("my-org"))
            .project(Property.ofValue("my-project"))
            .baseUrl(Property.ofValue(baseUrl))
            .personalAccessToken(Property.ofValue("test-pat-token"))
            .pipelineId(Property.ofValue(42))
            .runId(Property.ofValue(101))
            .build();

        var runContext = runContextFactory.of();
        var output = task.run(runContext);

        assertThat(output.getId(), is(101));
        assertThat(output.getName(), is("20261020.1"));
        assertThat(output.getState(), is("completed"));
        assertThat(output.getResult(), is("succeeded"));
        assertThat(output.getWebUrl(), containsString("buildId=101"));
    }

    @Test
    void testListRuns() throws Exception {
        wireMock.stubFor(
            get(urlPathEqualTo("/my-org/my-project/_apis/build/builds"))
                .withQueryParam("definitions", equalTo("42"))
                .withQueryParam("$top", equalTo("50"))
                .willReturn(okJson("""
                    {
                      "count": 2,
                      "value": [
                        {
                          "id": 101,
                          "buildNumber": "20261020.1",
                          "status": "completed",
                          "result": "succeeded",
                          "startTime": "2026-10-20T00:00:00Z",
                          "finishTime": "2026-10-20T00:05:00Z",
                          "_links": {
                            "web": {
                              "href": "https://dev.azure.com/my-org/my-project/_build/results?buildId=101"
                            }
                          }
                        },
                        {
                          "id": 102,
                          "buildNumber": "20261020.2",
                          "status": "inProgress",
                          "startTime": "2026-10-20T00:10:00Z"
                        }
                      ]
                    }
                    """))
        );

        var task = ListRuns.builder()
            .organization(Property.ofValue("my-org"))
            .project(Property.ofValue("my-project"))
            .baseUrl(Property.ofValue(baseUrl))
            .personalAccessToken(Property.ofValue("test-pat-token"))
            .pipelineId(Property.ofValue(42))
            .state(Property.ofValue("completed"))
            .build();

        var runContext = runContextFactory.of();
        var output = task.run(runContext);

        assertThat(output.getTotal(), is(1));
        assertThat(output.getRuns().getFirst().getId(), is(101));
        assertThat(output.getRuns().getFirst().getState(), is("completed"));
    }

    @Test
    void testRunPipelineNoWait() throws Exception {
        wireMock.stubFor(
            post(urlPathEqualTo("/my-org/my-project/_apis/pipelines/42/runs"))
                .willReturn(okJson("""
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
                    """))
        );

        var task = Run.builder()
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

        var runContext = runContextFactory.of();
        var output = task.run(runContext);

        assertThat(output.getId(), is(103));
        assertThat(output.getState(), is("completed"));
        assertThat(output.getResult(), is("succeeded"));
    }

    @Test
    void testRunPipelineWaitProgressionSuccess() throws Exception {
        wireMock.stubFor(
            post(urlPathEqualTo("/my-org/my-project/_apis/pipelines/42/runs"))
                .willReturn(okJson("""
                    {
                      "id": 105,
                      "name": "20261020.5",
                      "state": "inProgress",
                      "createdDate": "2026-10-20T00:20:00Z"
                    }
                    """))
        );

        wireMock.stubFor(
            get(urlPathEqualTo("/my-org/my-project/_apis/pipelines/42/runs/105"))
                .inScenario("PollingRun")
                .whenScenarioStateIs(Scenario.STARTED)
                .willSetStateTo("FirstPoll")
                .willReturn(okJson("""
                    {
                      "id": 105,
                      "name": "20261020.5",
                      "state": "inProgress"
                    }
                    """))
        );

        wireMock.stubFor(
            get(urlPathEqualTo("/my-org/my-project/_apis/pipelines/42/runs/105"))
                .inScenario("PollingRun")
                .whenScenarioStateIs("FirstPoll")
                .willReturn(okJson("""
                    {
                      "id": 105,
                      "name": "20261020.5",
                      "state": "completed",
                      "result": "succeeded",
                      "createdDate": "2026-10-20T00:20:00Z",
                      "finishedDate": "2026-10-20T00:21:00Z"
                    }
                    """))
        );

        var task = Run.builder()
            .organization(Property.ofValue("my-org"))
            .project(Property.ofValue("my-project"))
            .baseUrl(Property.ofValue(baseUrl))
            .personalAccessToken(Property.ofValue("test-pat-token"))
            .pipelineId(Property.ofValue(42))
            .wait(Property.ofValue(true))
            .pollInterval(Property.ofValue(Duration.ofMillis(20)))
            .maxDuration(Property.ofValue(Duration.ofSeconds(3)))
            .build();

        var output = task.run(runContextFactory.of());

        assertThat(output.getId(), is(105));
        assertThat(output.getState(), is("completed"));
        assertThat(output.getResult(), is("succeeded"));
    }

    @Test
    void testRunPipelineWaitFailedThrows() {
        wireMock.stubFor(
            post(urlPathEqualTo("/my-org/my-project/_apis/pipelines/42/runs"))
                .willReturn(okJson("""
                    {
                      "id": 106,
                      "name": "20261020.6",
                      "state": "inProgress"
                    }
                    """))
        );

        wireMock.stubFor(
            get(urlPathEqualTo("/my-org/my-project/_apis/pipelines/42/runs/106"))
                .willReturn(okJson("""
                    {
                      "id": 106,
                      "name": "20261020.6",
                      "state": "completed",
                      "result": "failed"
                    }
                    """))
        );

        var task = Run.builder()
            .organization(Property.ofValue("my-org"))
            .project(Property.ofValue("my-project"))
            .baseUrl(Property.ofValue(baseUrl))
            .personalAccessToken(Property.ofValue("test-pat-token"))
            .pipelineId(Property.ofValue(42))
            .wait(Property.ofValue(true))
            .pollInterval(Property.ofValue(Duration.ofMillis(20)))
            .maxDuration(Property.ofValue(Duration.ofSeconds(2)))
            .build();

        var ex = assertThrows(IllegalStateException.class, () -> task.run(runContextFactory.of()));
        assertThat(ex.getMessage(), containsString("completed with non-successful result: failed"));
    }

    @Test
    void testRunPipelineTimeoutTriggersCancel() throws Exception {
        wireMock.stubFor(
            post(urlPathEqualTo("/my-org/my-project/_apis/pipelines/42/runs"))
                .willReturn(okJson("""
                    {
                      "id": 107,
                      "name": "20261020.7",
                      "state": "inProgress"
                    }
                    """))
        );

        wireMock.stubFor(
            get(urlPathEqualTo("/my-org/my-project/_apis/pipelines/42/runs/107"))
                .willReturn(okJson("""
                    {
                      "id": 107,
                      "name": "20261020.7",
                      "state": "inProgress"
                    }
                    """))
        );

        wireMock.stubFor(
            patch(urlPathEqualTo("/my-org/my-project/_apis/build/builds/107"))
                .willReturn(okJson("""
                    {
                      "id": 107,
                      "status": "cancelling"
                    }
                    """))
        );

        var task = Run.builder()
            .organization(Property.ofValue("my-org"))
            .project(Property.ofValue("my-project"))
            .baseUrl(Property.ofValue(baseUrl))
            .personalAccessToken(Property.ofValue("test-pat-token"))
            .pipelineId(Property.ofValue(42))
            .wait(Property.ofValue(true))
            .pollInterval(Property.ofValue(Duration.ofMillis(15)))
            .maxDuration(Property.ofValue(Duration.ofMillis(50)))
            .build();

        var ex = assertThrows(IllegalStateException.class, () -> task.run(runContextFactory.of()));
        assertThat(ex.getMessage(), containsString("did not complete within timeout"));

        verifyWithPolling(
            patchRequestedFor(urlPathEqualTo("/my-org/my-project/_apis/build/builds/107"))
                .withRequestBody(containing("\"Cancelling\""))
        );
    }

    @Test
    void testKillTriggersCancel() throws Exception {
        wireMock.stubFor(
            post(urlPathEqualTo("/my-org/my-project/_apis/pipelines/42/runs"))
                .willReturn(okJson("""
                    {
                      "id": 108,
                      "name": "20261020.8",
                      "state": "inProgress"
                    }
                    """))
        );

        wireMock.stubFor(
            patch(urlPathEqualTo("/my-org/my-project/_apis/build/builds/108"))
                .willReturn(okJson("""
                    {
                      "id": 108,
                      "status": "cancelling"
                    }
                    """))
        );

        var task = Run.builder()
            .organization(Property.ofValue("my-org"))
            .project(Property.ofValue("my-project"))
            .baseUrl(Property.ofValue(baseUrl))
            .personalAccessToken(Property.ofValue("test-pat-token"))
            .pipelineId(Property.ofValue(42))
            .wait(Property.ofValue(false))
            .build();

        task.run(runContextFactory.of());
        task.kill();

        verifyWithPolling(
            patchRequestedFor(urlPathEqualTo("/my-org/my-project/_apis/build/builds/108"))
                .withRequestBody(containing("\"Cancelling\""))
        );
    }

    private void verifyWithPolling(com.github.tomakehurst.wiremock.matching.RequestPatternBuilder patternBuilder) throws Exception {
        long end = System.currentTimeMillis() + 3000;
        while (System.currentTimeMillis() < end) {
            if (!wireMock.findAll(patternBuilder).isEmpty()) {
                return;
            }
            Thread.sleep(25);
        }
        wireMock.verify(patternBuilder);
    }

    @Test
    void testCancel() throws Exception {
        wireMock.stubFor(
            patch(urlPathEqualTo("/my-org/my-project/_apis/build/builds/101"))
                .willReturn(okJson("""
                    {
                      "id": 101,
                      "status": "cancelling"
                    }
                    """))
        );

        var task = Cancel.builder()
            .organization(Property.ofValue("my-org"))
            .project(Property.ofValue("my-project"))
            .baseUrl(Property.ofValue(baseUrl))
            .personalAccessToken(Property.ofValue("test-pat-token"))
            .runId(Property.ofValue(101))
            .build();

        task.run(runContextFactory.of());

        wireMock.verify(
            patchRequestedFor(urlPathEqualTo("/my-org/my-project/_apis/build/builds/101"))
                .withRequestBody(containing("\"Cancelling\""))
        );
    }

    @Test
    void testProjectNameWithSpaceIsUrlEncoded() throws Exception {
        wireMock.stubFor(
            get(urlPathEqualTo("/my-org/My%20Project/_apis/pipelines/42"))
                .willReturn(okJson("""
                    {
                      "id": 42,
                      "name": "MyPipeline",
                      "folder": "\\\\",
                      "revision": 1
                    }
                    """))
        );

        var task = Get.builder()
            .organization(Property.ofValue("my-org"))
            .project(Property.ofValue("My Project"))
            .baseUrl(Property.ofValue(baseUrl))
            .personalAccessToken(Property.ofValue("test-pat-token"))
            .pipelineId(Property.ofValue(42))
            .build();

        var output = task.run(runContextFactory.of());

        assertThat(output.getId(), is(42));
        assertThat(output.getName(), is("MyPipeline"));
        wireMock.verify(getRequestedFor(urlPathEqualTo("/my-org/My%20Project/_apis/pipelines/42")));
    }

    @Test
    void testListOrderByUrlEncoded() throws Exception {
        wireMock.stubFor(
            get(urlPathEqualTo("/my-org/my-project/_apis/pipelines"))
                .withQueryParam("orderBy", equalTo("name asc"))
                .willReturn(okJson("""
                    {
                      "count": 1,
                      "value": [
                        {
                          "id": 42,
                          "name": "MyPipeline"
                        }
                      ]
                    }
                    """))
        );

        var task = List.builder()
            .organization(Property.ofValue("my-org"))
            .project(Property.ofValue("my-project"))
            .baseUrl(Property.ofValue(baseUrl))
            .personalAccessToken(Property.ofValue("test-pat-token"))
            .orderBy(Property.ofValue("name asc"))
            .build();

        var output = task.run(runContextFactory.of());

        assertThat(output.getTotal(), is(1));
        assertThat(output.getPipelines().getFirst().getId(), is(42));
    }

    @Test
    void testTriggerEvaluationWithKVWatermark() throws Exception {
        wireMock.stubFor(
            get(urlPathEqualTo("/my-org/my-project/_apis/build/builds"))
                .withQueryParam("definitions", equalTo("42"))
                .withQueryParam("$top", equalTo("25"))
                .willReturn(okJson("""
                    {
                      "count": 1,
                      "value": [
                        {
                          "id": 101,
                          "buildNumber": "20261020.1",
                          "status": "completed",
                          "result": "succeeded",
                          "finishTime": "2026-10-20T00:05:00Z"
                        }
                      ]
                    }
                    """))
        );

        var trigger = Trigger.builder()
            .id("trg-" + IdUtils.create())
            .type(Trigger.class.getName())
            .organization(Property.ofValue("my-org"))
            .project(Property.ofValue("my-project"))
            .baseUrl(Property.ofValue(baseUrl))
            .personalAccessToken(Property.ofValue("test-pat-token"))
            .pipelineId(Property.ofValue(42))
            .interval(Duration.ofSeconds(60))
            .build();

        var entry = TestsUtils.mockTrigger(runContextFactory, trigger);
        var conditionContext = entry.getKey();
        var triggerContext = entry.getValue().context();

        // First poll: observes historical run 101, seeds watermark, does not fire
        var first = trigger.evaluate(conditionContext, triggerContext);
        assertThat(first.isEmpty(), is(true));

        // Stub a new run (id 102) with a newer finishTime
        wireMock.stubFor(
            get(urlPathEqualTo("/my-org/my-project/_apis/build/builds"))
                .withQueryParam("definitions", equalTo("42"))
                .withQueryParam("$top", equalTo("25"))
                .willReturn(okJson("""
                    {
                      "count": 2,
                      "value": [
                        {
                          "id": 102,
                          "buildNumber": "20261020.2",
                          "status": "completed",
                          "result": "succeeded",
                          "finishTime": "2026-10-20T00:10:00Z"
                        },
                        {
                          "id": 101,
                          "buildNumber": "20261020.1",
                          "status": "completed",
                          "result": "succeeded",
                          "finishTime": "2026-10-20T00:05:00Z"
                        }
                      ]
                    }
                    """))
        );

        // Second poll: observes new run 102 -> fires Execution
        var second = trigger.evaluate(conditionContext, triggerContext);
        assertThat(second.isPresent(), is(true));
        assertThat(second.get().getTrigger().getVariables().get("id"), is(102));

        // Third poll: same state -> does not refire
        var third = trigger.evaluate(conditionContext, triggerContext);
        assertThat(third.isEmpty(), is(true));
    }

    @Test
    void testMissingCredentialsThrows() {
        var task = GetRun.builder()
            .organization(Property.ofValue("my-org"))
            .project(Property.ofValue("my-project"))
            .baseUrl(Property.ofValue(baseUrl))
            .pipelineId(Property.ofValue(42))
            .runId(Property.ofValue(101))
            .build();

        var runContext = runContextFactory.of();
        assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
    }

    @Test
    void testGetPipeline() throws Exception {
        wireMock.stubFor(
            get(urlPathEqualTo("/my-org/my-project/_apis/pipelines/42"))
                .willReturn(okJson("""
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
                    """.formatted(baseUrl)))
        );

        var task = Get.builder()
            .organization(Property.ofValue("my-org"))
            .project(Property.ofValue("my-project"))
            .baseUrl(Property.ofValue(baseUrl))
            .personalAccessToken(Property.ofValue("test-pat-token"))
            .pipelineId(Property.ofValue(42))
            .build();

        var output = task.run(runContextFactory.of());

        assertThat(output.getId(), is(42));
        assertThat(output.getName(), is("MyPipeline"));
        assertThat(output.getFolder(), is("\\"));
        assertThat(output.getRevision(), is(2));
        assertThat(output.getWebUrl(), containsString("definitionId=42"));
    }

    @Test
    void testListPipelines() throws Exception {
        wireMock.stubFor(
            get(urlPathEqualTo("/my-org/my-project/_apis/pipelines"))
                .willReturn(okJson("""
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
                    """))
        );

        var task = List.builder()
            .organization(Property.ofValue("my-org"))
            .project(Property.ofValue("my-project"))
            .baseUrl(Property.ofValue(baseUrl))
            .personalAccessToken(Property.ofValue("test-pat-token"))
            .build();

        var output = task.run(runContextFactory.of());

        assertThat(output.getTotal(), is(1));
        assertThat(output.getPipelines().getFirst().getId(), is(42));
        assertThat(output.getPipelines().getFirst().getName(), is("MyPipeline"));
    }

    @Test
    void testCreatePipeline() throws Exception {
        wireMock.stubFor(
            post(urlPathEqualTo("/my-org/my-project/_apis/pipelines"))
                .willReturn(okJson("""
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
                    """))
        );

        var task = Create.builder()
            .organization(Property.ofValue("my-org"))
            .project(Property.ofValue("my-project"))
            .baseUrl(Property.ofValue(baseUrl))
            .personalAccessToken(Property.ofValue("test-pat-token"))
            .name(Property.ofValue("MyNewPipeline"))
            .folder(Property.ofValue("\\"))
            .configuration(Property.ofValue(Map.of("type", "yaml", "path", "azure-pipelines.yml")))
            .build();

        var output = task.run(runContextFactory.of());

        assertThat(output.getId(), is(43));
        assertThat(output.getName(), is("MyNewPipeline"));
        assertThat(output.getFolder(), is("\\"));
        assertThat(output.getRevision(), is(1));
        assertThat(output.getWebUrl(), containsString("definitionId=43"));
    }

    @Test
    void shouldNotSendEntraTokenToCustomHost() {
        var task = Get.builder()
            .organization(Property.ofValue("my-org"))
            .project(Property.ofValue("my-project"))
            .baseUrl(Property.ofValue(baseUrl))
            .tenantId(Property.ofValue("tenant"))
            .clientId(Property.ofValue("client"))
            .pipelineId(Property.ofValue(42))
            .build();

        assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));
    }
}
