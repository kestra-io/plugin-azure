package io.kestra.plugin.azure.aifoundry;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import com.azure.ai.agents.persistent.MessagesClient;
import com.azure.ai.agents.persistent.PersistentAgentsClient;
import com.azure.ai.agents.persistent.RunsClient;
import com.azure.ai.agents.persistent.ThreadsClient;
import com.azure.ai.agents.persistent.models.PersistentAgentThread;
import com.azure.ai.agents.persistent.models.CreateRunOptions;
import com.azure.ai.agents.persistent.models.MessageRole;
import com.azure.ai.agents.persistent.models.MessageTextContent;
import com.azure.ai.agents.persistent.models.MessageTextDetails;

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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.eq;

@KestraTest
class RunAgentTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run_pollsUntilCompleteAndReturnsMessage() throws Exception {
        RunAgent task = RunAgent.builder()
            .id("run-agent")
            .type(RunAgent.class.getName())
            .endpoint(Property.ofValue("https://test.api.azureml.ms/"))
            .agentId(Property.ofValue("agent-123"))
            .prompt(Property.ofValue("Hello agent"))
            .pollInterval(Property.ofValue(Duration.ofMillis(1))) // extremely fast polling for test
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        // Mocks for thread creation
        PersistentAgentThread mockThread = mock(PersistentAgentThread.class);
        when(mockThread.getId()).thenReturn("thread-123");
        ThreadsClient threadsClient = mock(ThreadsClient.class);
        when(threadsClient.createThread()).thenReturn(mockThread);

        // Mocks for posting message
        MessagesClient messagesClient = mock(MessagesClient.class);
        ThreadMessage mockCreatedMessage = mock(ThreadMessage.class);
        when(messagesClient.createMessage(eq("thread-123"), eq(MessageRole.USER), eq("Hello agent")))
            .thenReturn(mockCreatedMessage);

        // Mocks for creating run
        ThreadRun mockRunCreated = mock(ThreadRun.class);
        when(mockRunCreated.getId()).thenReturn("run-456");

        // Mocks for polling run status
        ThreadRun mockRunInProgress = mock(ThreadRun.class);
        when(mockRunInProgress.getStatus()).thenReturn(RunStatus.IN_PROGRESS);

        ThreadRun mockRunCompleted = mock(ThreadRun.class);
        when(mockRunCompleted.getStatus()).thenReturn(RunStatus.COMPLETED);

        RunsClient runsClient = mock(RunsClient.class);
        when(runsClient.createRun(any(CreateRunOptions.class))).thenReturn(mockRunCreated);
        
        // Return IN_PROGRESS twice, then COMPLETED
        when(runsClient.getRun("thread-123", "run-456"))
            .thenReturn(mockRunInProgress)
            .thenReturn(mockRunInProgress)
            .thenReturn(mockRunCompleted);

        // Mocks for retrieving assistant response
        MessageTextDetails textDetails = mock(MessageTextDetails.class);
        when(textDetails.getValue()).thenReturn("Agent reply");
        MessageTextContent textContent = mock(MessageTextContent.class);
        when(textContent.getText()).thenReturn(textDetails);

        ThreadMessage mockAssistantMessage = mock(ThreadMessage.class);
        when(mockAssistantMessage.getRole()).thenReturn(MessageRole.AGENT);
        when(mockAssistantMessage.getContent()).thenReturn(List.of(textContent));

        
        
        
        PagedIterable<ThreadMessage> pagedIterable = mock(PagedIterable.class);
        when(pagedIterable.stream()).thenReturn(java.util.stream.Stream.of(mockAssistantMessage));
        
        when(messagesClient.listMessages("thread-123")).thenReturn(pagedIterable);

        PersistentAgentsClient agentsClient = mock(PersistentAgentsClient.class);
        when(agentsClient.getThreadsClient()).thenReturn(threadsClient);
        when(agentsClient.getMessagesClient()).thenReturn(messagesClient);
        when(agentsClient.getRunsClient()).thenReturn(runsClient);

        try (MockedConstruction<AIProjectClientBuilder> ignored =
                 Mockito.mockConstruction(AIProjectClientBuilder.class, (mock, ctx) -> {
                     when(mock.endpoint(anyString())).thenReturn(mock);
                     when(mock.credential(any())).thenReturn(mock);
                     when(mock.buildPersistentAgentsClient()).thenReturn(agentsClient);
                 })) {

            RunAgent.Output output = task.run(runContext);

            assertThat(output, notNullValue());
            assertThat(output.getResult(), is("Agent reply"));
            assertThat(output.getThreadId(), is("thread-123"));
            assertThat(output.getRunId(), is("run-456"));

            // Verify important arguments passed to the SDK
            verify(messagesClient).createMessage("thread-123", MessageRole.USER, "Hello agent");
            
            ArgumentCaptor<CreateRunOptions> runOptionsCaptor = ArgumentCaptor.forClass(CreateRunOptions.class);
            verify(runsClient).createRun(runOptionsCaptor.capture());
            assertThat(runOptionsCaptor.getValue().getThreadId(), is("thread-123"));
            assertThat(runOptionsCaptor.getValue().getAssistantId(), is("agent-123"));
            
            // Verify polling loop occurred (1 initial check inside loop structure, maybe more depending on while condition)
            // It should be called 3 times total based on our mock setup
            verify(runsClient, times(3)).getRun("thread-123", "run-456");
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

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

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
        when(runsClient.createRun(any(CreateRunOptions.class))).thenReturn(mockRunCreated);
        when(runsClient.getRun("thread-123", "run-456")).thenReturn(mockRunFailed);

        PersistentAgentsClient agentsClient = mock(PersistentAgentsClient.class);
        when(agentsClient.getThreadsClient()).thenReturn(threadsClient);
        when(agentsClient.getMessagesClient()).thenReturn(messagesClient);
        when(agentsClient.getRunsClient()).thenReturn(runsClient);

        try (MockedConstruction<AIProjectClientBuilder> ignored =
                 Mockito.mockConstruction(AIProjectClientBuilder.class, (mock, ctx) -> {
                     when(mock.endpoint(anyString())).thenReturn(mock);
                     when(mock.credential(any())).thenReturn(mock);
                     when(mock.buildPersistentAgentsClient()).thenReturn(agentsClient);
                 })) {

            assertThrows(IllegalStateException.class, () -> task.run(runContext));
        }
    }
}
