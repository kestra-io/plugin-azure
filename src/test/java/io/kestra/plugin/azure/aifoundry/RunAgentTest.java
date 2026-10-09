
package io.kestra.plugin.azure.aifoundry;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import com.azure.ai.agents.persistent.MessagesClient;
import com.azure.ai.agents.persistent.PersistentAgentsClient;
import com.azure.ai.agents.persistent.RunsClient;
import com.azure.ai.agents.persistent.ThreadsClient;
import com.azure.ai.agents.persistent.models.CreateRunOptions;
import com.azure.ai.agents.persistent.models.ListSortOrder;
import com.azure.ai.agents.persistent.models.MessageRole;
import com.azure.ai.agents.persistent.models.MessageTextContent;
import com.azure.ai.agents.persistent.models.MessageTextDetails;
import com.azure.ai.agents.persistent.models.PersistentAgentThread;
import com.azure.ai.agents.persistent.models.RunStatus;
import com.azure.ai.agents.persistent.models.ThreadMessage;
import com.azure.ai.agents.persistent.models.ThreadRun;
import com.azure.ai.projects.AIProjectClientBuilder;
import com.azure.core.http.rest.PagedIterable;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@KestraTest
class RunAgentTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run_withApiKey_throwsIllegalArgumentWithGuidance() throws Exception {
        RunAgent task = RunAgent.builder()
            .id("run-agent")
            .type(RunAgent.class.getName())
            .endpoint(Property.ofValue("https://test.api.azureml.ms/"))
            .apiKey(Property.ofValue("some-key"))
            .agentId(Property.ofValue("agent-123"))
            .prompt(Property.ofValue("Hello agent"))
            .build();

        RunContext runContext =
            TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> task.run(runContext)
        );

        assertThat(ex.getMessage(), containsString("Entra ID"));
    }

    @Test
    void run_pollsUntilCompleteAndReturnsMessage() throws Exception {
        RunAgent task = RunAgent.builder()
            .id("run-agent")
            .type(RunAgent.class.getName())
            .endpoint(Property.ofValue("https://test.api.azureml.ms/"))
            .agentId(Property.ofValue("agent-123"))
            .prompt(Property.ofValue("Hello agent"))
            .pollInterval(Property.ofValue(Duration.ofMillis(1)))
            .build();

        RunContext runContext =
            TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        PersistentAgentThread mockThread = mock(PersistentAgentThread.class);
        when(mockThread.getId()).thenReturn("thread-123");

        ThreadsClient threadsClient = mock(ThreadsClient.class);
        when(threadsClient.createThread()).thenReturn(mockThread);

        MessagesClient messagesClient = mock(MessagesClient.class);
        ThreadMessage mockCreatedMessage = mock(ThreadMessage.class);

        when(messagesClient.createMessage(
            eq("thread-123"),
            eq(MessageRole.USER),
            eq("Hello agent")
        )).thenReturn(mockCreatedMessage);

        ThreadRun mockRunCreated = mock(ThreadRun.class);
        when(mockRunCreated.getId()).thenReturn("run-456");

        ThreadRun mockRunInProgress = mock(ThreadRun.class);
        when(mockRunInProgress.getStatus()).thenReturn(RunStatus.IN_PROGRESS);

        ThreadRun mockRunCompleted = mock(ThreadRun.class);
        when(mockRunCompleted.getStatus()).thenReturn(RunStatus.COMPLETED);

        RunsClient runsClient = mock(RunsClient.class);
        when(runsClient.createRun(any(CreateRunOptions.class)))
            .thenReturn(mockRunCreated);

        when(runsClient.getRun("thread-123", "run-456"))
            .thenReturn(mockRunInProgress)
            .thenReturn(mockRunInProgress)
            .thenReturn(mockRunCompleted);

        MessageTextDetails textDetails = mock(MessageTextDetails.class);
        when(textDetails.getValue()).thenReturn("Agent reply");

        MessageTextContent textContent = mock(MessageTextContent.class);
        when(textContent.getText()).thenReturn(textDetails);

        ThreadMessage mockAssistantMessage = mock(ThreadMessage.class);
        when(mockAssistantMessage.getRole()).thenReturn(MessageRole.AGENT);
        when(mockAssistantMessage.getContent()).thenReturn(List.of(textContent));

        @SuppressWarnings("unchecked")
        PagedIterable<ThreadMessage> pagedIterable = mock(PagedIterable.class);

        when(pagedIterable.stream())
            .thenReturn(java.util.stream.Stream.of(mockAssistantMessage));

        when(messagesClient.listMessages(
            eq("thread-123"),
            isNull(),
            isNull(),
            eq(ListSortOrder.DESCENDING),
            isNull(),
            isNull()
        )).thenReturn(pagedIterable);

        PersistentAgentsClient agentsClient = mock(PersistentAgentsClient.class);
        when(agentsClient.getThreadsClient()).thenReturn(threadsClient);
        when(agentsClient.getMessagesClient()).thenReturn(messagesClient);
        when(agentsClient.getRunsClient()).thenReturn(runsClient);

        try (MockedConstruction<AIProjectClientBuilder> ignored =
                 Mockito.mockConstruction(
                     AIProjectClientBuilder.class,
                     (builder, context) -> {
                         when(builder.endpoint(anyString())).thenReturn(builder);
                         when(builder.credential(any())).thenReturn(builder);
                         when(builder.buildPersistentAgentsClient())
                             .thenReturn(agentsClient);
                     }
                 )) {

            RunAgent.Output output = task.run(runContext);

            assertThat(output, notNullValue());
            assertThat(output.getResult(), is("Agent reply"));
            assertThat(output.getThreadId(), is("thread-123"));
            assertThat(output.getRunId(), is("run-456"));

            verify(messagesClient).createMessage(
                "thread-123",
                MessageRole.USER,
                "Hello agent"
            );

            ArgumentCaptor<CreateRunOptions> runOptionsCaptor =
                ArgumentCaptor.forClass(CreateRunOptions.class);

            verify(runsClient).createRun(runOptionsCaptor.capture());

            assertThat(
                runOptionsCaptor.getValue().getThreadId(),
                is("thread-123")
            );
            assertThat(
                runOptionsCaptor.getValue().getAssistantId(),
                is("agent-123")
            );

            verify(runsClient, times(3))
                .getRun("thread-123", "run-456");
        }
    }

    @Test
    void kill_beforeRunStarts_requestsAzureCancellation() throws Exception {
        RunAgent task = RunAgent.builder()
            .id("run-agent")
            .type(RunAgent.class.getName())
            .endpoint(Property.ofValue("https://test.api.azureml.ms/"))
            .agentId(Property.ofValue("agent-123"))
            .prompt(Property.ofValue("Hello agent"))
            .pollInterval(Property.ofValue(Duration.ofMillis(1)))
            .build();

        RunContext runContext =
            TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        // Request cancellation before the Azure run ID exists.
        task.kill();

        PersistentAgentThread mockThread = mock(PersistentAgentThread.class);
        when(mockThread.getId()).thenReturn("thread-123");

        ThreadsClient threadsClient = mock(ThreadsClient.class);
        when(threadsClient.createThread()).thenReturn(mockThread);

        MessagesClient messagesClient = mock(MessagesClient.class);

        ThreadRun createdRun = mock(ThreadRun.class);
        when(createdRun.getId()).thenReturn("run-456");

        ThreadRun cancelledRun = mock(ThreadRun.class);
        when(cancelledRun.getStatus()).thenReturn(RunStatus.CANCELLED);

        RunsClient runsClient = mock(RunsClient.class);
        when(runsClient.createRun(any(CreateRunOptions.class)))
            .thenReturn(createdRun);

        when(runsClient.getRun("thread-123", "run-456"))
            .thenReturn(cancelledRun);

        PersistentAgentsClient agentsClient = mock(PersistentAgentsClient.class);
        when(agentsClient.getThreadsClient()).thenReturn(threadsClient);
        when(agentsClient.getMessagesClient()).thenReturn(messagesClient);
        when(agentsClient.getRunsClient()).thenReturn(runsClient);

        try (MockedConstruction<AIProjectClientBuilder> ignored =
                 Mockito.mockConstruction(
                     AIProjectClientBuilder.class,
                     (builder, context) -> {
                         when(builder.endpoint(anyString())).thenReturn(builder);
                         when(builder.credential(any())).thenReturn(builder);
                         when(builder.buildPersistentAgentsClient())
                             .thenReturn(agentsClient);
                     }
                 )) {

            assertThrows(
                IllegalStateException.class,
                () -> task.run(runContext)
            );

            // The cancellation call runs asynchronously.
            verify(runsClient, timeout(2_000))
                .cancelRun("thread-123", "run-456");
        }
    }

    @Test
    void kill_duringPolling_requestsAzureCancellation() throws Exception {
        RunAgent task = RunAgent.builder()
            .id("run-agent")
            .type(RunAgent.class.getName())
            .endpoint(Property.ofValue("https://test.api.azureml.ms/"))
            .agentId(Property.ofValue("agent-123"))
            .prompt(Property.ofValue("Hello agent"))
            .pollInterval(Property.ofValue(Duration.ofMillis(10)))
            .timeout(Property.ofValue(Duration.ofSeconds(5)))
            .build();

        RunContext runContext =
            TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        PersistentAgentThread mockThread = mock(PersistentAgentThread.class);
        when(mockThread.getId()).thenReturn("thread-123");

        ThreadsClient threadsClient = mock(ThreadsClient.class);
        when(threadsClient.createThread()).thenReturn(mockThread);

        MessagesClient messagesClient = mock(MessagesClient.class);

        ThreadRun createdRun = mock(ThreadRun.class);
        when(createdRun.getId()).thenReturn("run-456");
        when(createdRun.getStatus()).thenReturn(RunStatus.IN_PROGRESS);

        ThreadRun pollingRun = mock(ThreadRun.class);
        AtomicBoolean cancelled = new AtomicBoolean(false);
        CountDownLatch pollingStarted = new CountDownLatch(1);

        when(pollingRun.getStatus()).thenAnswer(invocation ->
            cancelled.get() ? RunStatus.CANCELLED : RunStatus.IN_PROGRESS
        );

        RunsClient runsClient = mock(RunsClient.class);
        when(runsClient.createRun(any(CreateRunOptions.class)))
            .thenReturn(createdRun);

        when(runsClient.getRun("thread-123", "run-456"))
            .thenAnswer(invocation -> {
                pollingStarted.countDown();
                return pollingRun;
            });

        Mockito.doAnswer(invocation -> {
            cancelled.set(true);
            return pollingRun;
        }).when(runsClient).cancelRun("thread-123", "run-456");

        PersistentAgentsClient agentsClient = mock(PersistentAgentsClient.class);
        when(agentsClient.getThreadsClient()).thenReturn(threadsClient);
        when(agentsClient.getMessagesClient()).thenReturn(messagesClient);
        when(agentsClient.getRunsClient()).thenReturn(runsClient);

        try (MockedConstruction<AIProjectClientBuilder> ignored =
                 Mockito.mockConstruction(
                     AIProjectClientBuilder.class,
                     (builder, context) -> {
                         when(builder.endpoint(anyString())).thenReturn(builder);
                         when(builder.credential(any())).thenReturn(builder);
                         when(builder.buildPersistentAgentsClient())
                             .thenReturn(agentsClient);
                     }
                 )) {

            Thread killer = new Thread(() -> {
                try {
                    if (pollingStarted.await(2, TimeUnit.SECONDS)) {
                        task.kill();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            killer.start();

            assertThrows(
                IllegalStateException.class,
                () -> task.run(runContext)
            );

            killer.join(2_000);

            verify(runsClient, timeout(2_000))
                .cancelRun("thread-123", "run-456");
        }
    }

    @Test
    void kill_afterCompletion_doesNotCancelAzureRun() throws Exception {
        RunAgent task = RunAgent.builder()
            .id("run-agent")
            .type(RunAgent.class.getName())
            .endpoint(Property.ofValue("https://test.api.azureml.ms/"))
            .agentId(Property.ofValue("agent-123"))
            .prompt(Property.ofValue("Hello agent"))
            .pollInterval(Property.ofValue(Duration.ofMillis(1)))
            .build();

        RunContext runContext =
            TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        PersistentAgentThread mockThread = mock(PersistentAgentThread.class);
        when(mockThread.getId()).thenReturn("thread-123");

        ThreadsClient threadsClient = mock(ThreadsClient.class);
        when(threadsClient.createThread()).thenReturn(mockThread);

        MessagesClient messagesClient = mock(MessagesClient.class);
        ThreadMessage mockCreatedMessage = mock(ThreadMessage.class);

        when(messagesClient.createMessage(
            eq("thread-123"),
            eq(MessageRole.USER),
            eq("Hello agent")
        )).thenReturn(mockCreatedMessage);

        ThreadRun createdRun = mock(ThreadRun.class);
        when(createdRun.getId()).thenReturn("run-456");

        ThreadRun completedRun = mock(ThreadRun.class);
        when(completedRun.getStatus()).thenReturn(RunStatus.COMPLETED);

        RunsClient runsClient = mock(RunsClient.class);
        when(runsClient.createRun(any(CreateRunOptions.class)))
            .thenReturn(createdRun);

        when(runsClient.getRun("thread-123", "run-456"))
            .thenReturn(completedRun);

        MessageTextDetails textDetails = mock(MessageTextDetails.class);
        when(textDetails.getValue()).thenReturn("Agent reply");

        MessageTextContent textContent = mock(MessageTextContent.class);
        when(textContent.getText()).thenReturn(textDetails);

        ThreadMessage assistantMessage = mock(ThreadMessage.class);
        when(assistantMessage.getRole()).thenReturn(MessageRole.AGENT);
        when(assistantMessage.getContent()).thenReturn(List.of(textContent));

        @SuppressWarnings("unchecked")
        PagedIterable<ThreadMessage> pagedIterable = mock(PagedIterable.class);

        when(pagedIterable.stream())
            .thenReturn(java.util.stream.Stream.of(assistantMessage));

        when(messagesClient.listMessages(
            eq("thread-123"),
            isNull(),
            isNull(),
            eq(ListSortOrder.DESCENDING),
            isNull(),
            isNull()
        )).thenReturn(pagedIterable);

        PersistentAgentsClient agentsClient = mock(PersistentAgentsClient.class);
        when(agentsClient.getThreadsClient()).thenReturn(threadsClient);
        when(agentsClient.getMessagesClient()).thenReturn(messagesClient);
        when(agentsClient.getRunsClient()).thenReturn(runsClient);

        try (MockedConstruction<AIProjectClientBuilder> ignored =
                 Mockito.mockConstruction(
                     AIProjectClientBuilder.class,
                     (builder, context) -> {
                         when(builder.endpoint(anyString())).thenReturn(builder);
                         when(builder.credential(any())).thenReturn(builder);
                         when(builder.buildPersistentAgentsClient())
                             .thenReturn(agentsClient);
                     }
                 )) {

            task.run(runContext);

            task.kill();

            verify(runsClient, timeout(500).times(0))
                .cancelRun("thread-123", "run-456");
        }
    }

    @Test
    void run_timeout_requestsAzureCancellation() throws Exception {
        RunAgent task = RunAgent.builder()
            .id("run-agent")
            .type(RunAgent.class.getName())
            .endpoint(Property.ofValue("https://test.api.azureml.ms/"))
            .agentId(Property.ofValue("agent-123"))
            .prompt(Property.ofValue("Hello agent"))
            .pollInterval(Property.ofValue(Duration.ofMillis(10)))
            .timeout(Property.ofValue(Duration.ofMillis(20)))
            .build();

        RunContext runContext =
            TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        PersistentAgentThread mockThread = mock(PersistentAgentThread.class);
        when(mockThread.getId()).thenReturn("thread-123");

        ThreadsClient threadsClient = mock(ThreadsClient.class);
        when(threadsClient.createThread()).thenReturn(mockThread);

        MessagesClient messagesClient = mock(MessagesClient.class);

        ThreadRun createdRun = mock(ThreadRun.class);
        when(createdRun.getId()).thenReturn("run-456");
        when(createdRun.getStatus()).thenReturn(RunStatus.IN_PROGRESS);

        ThreadRun pollingRun = mock(ThreadRun.class);
        when(pollingRun.getStatus()).thenReturn(RunStatus.IN_PROGRESS);

        RunsClient runsClient = mock(RunsClient.class);
        when(runsClient.createRun(any(CreateRunOptions.class)))
            .thenReturn(createdRun);
        when(runsClient.getRun("thread-123", "run-456"))
            .thenReturn(pollingRun);

        PersistentAgentsClient agentsClient = mock(PersistentAgentsClient.class);
        when(agentsClient.getThreadsClient()).thenReturn(threadsClient);
        when(agentsClient.getMessagesClient()).thenReturn(messagesClient);
        when(agentsClient.getRunsClient()).thenReturn(runsClient);

        try (MockedConstruction<AIProjectClientBuilder> ignored =
                 Mockito.mockConstruction(
                     AIProjectClientBuilder.class,
                     (builder, context) -> {
                         when(builder.endpoint(anyString())).thenReturn(builder);
                         when(builder.credential(any())).thenReturn(builder);
                         when(builder.buildPersistentAgentsClient())
                             .thenReturn(agentsClient);
                     }
                 )) {

            assertThrows(
                IllegalStateException.class,
                () -> task.run(runContext)
            );

            verify(runsClient, timeout(2_000))
                .cancelRun("thread-123", "run-456");
        }
    }

    @Test
    void run_failedStatus_throwsException() throws Exception {
        RunAgent task = RunAgent.builder()
            .id("run-agent")
            .type(RunAgent.class.getName())
            .endpoint(Property.ofValue("https://test.api.azureml.ms/"))
            .agentId(Property.ofValue("agent-123"))
            .prompt(Property.ofValue("Hello agent"))
            .pollInterval(Property.ofValue(Duration.ofMillis(1)))
            .build();

        RunContext runContext =
            TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        PersistentAgentThread mockThread = mock(PersistentAgentThread.class);
        when(mockThread.getId()).thenReturn("thread-123");

        ThreadsClient threadsClient = mock(ThreadsClient.class);
        when(threadsClient.createThread()).thenReturn(mockThread);

        MessagesClient messagesClient = mock(MessagesClient.class);

        ThreadRun mockRunCreated = mock(ThreadRun.class);
        when(mockRunCreated.getId()).thenReturn("run-456");

        ThreadRun mockRunFailed = mock(ThreadRun.class);
        when(mockRunFailed.getStatus()).thenReturn(RunStatus.FAILED);

        RunsClient runsClient = mock(RunsClient.class);
        when(runsClient.createRun(any(CreateRunOptions.class)))
            .thenReturn(mockRunCreated);

        when(runsClient.getRun("thread-123", "run-456"))
            .thenReturn(mockRunFailed);

        PersistentAgentsClient agentsClient = mock(PersistentAgentsClient.class);
        when(agentsClient.getThreadsClient()).thenReturn(threadsClient);
        when(agentsClient.getMessagesClient()).thenReturn(messagesClient);
        when(agentsClient.getRunsClient()).thenReturn(runsClient);

        try (MockedConstruction<AIProjectClientBuilder> ignored =
                 Mockito.mockConstruction(
                     AIProjectClientBuilder.class,
                     (builder, context) -> {
                         when(builder.endpoint(anyString())).thenReturn(builder);
                         when(builder.credential(any())).thenReturn(builder);
                         when(builder.buildPersistentAgentsClient())
                             .thenReturn(agentsClient);
                     }
                 )) {

            assertThrows(
                IllegalStateException.class,
                () -> task.run(runContext)
            );
        }
    }
}