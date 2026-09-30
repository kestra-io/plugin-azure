package io.kestra.plugin.azure.aifoundry;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import com.azure.ai.inference.ChatCompletionsClient;
import com.azure.ai.inference.ChatCompletionsClientBuilder;
import com.azure.ai.inference.models.ChatChoice;
import com.azure.ai.inference.models.ChatCompletions;
import com.azure.ai.inference.models.ChatCompletionsOptions;
import com.azure.ai.inference.models.ChatRequestMessage;
import com.azure.ai.inference.models.ChatRequestUserMessage;
import com.azure.ai.inference.models.ChatResponseMessage;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@KestraTest
class ChatCompletionTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run_withMockedClient_returnsContentAndVerifiesArgs() throws Exception {
        ChatCompletion task = ChatCompletion.builder()
            .id("chat-completion")
            .type(ChatCompletion.class.getName())
            .endpoint(Property.ofValue("https://test.api.azureml.ms/"))
            .apiKey(Property.ofValue("test-key"))
            .deploymentName(Property.ofValue("gpt-4o"))
            .messages(
                Property.ofValue(
                    List.of(
                        new ChatCompletion.ChatMessage(Property.ofValue("user"), Property.ofValue("Hello Azure AI"))
                    )
                )
            )
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        ChatResponseMessage responseMessage = mock(ChatResponseMessage.class);
        when(responseMessage.getContent()).thenReturn("Hello from Azure!");

        ChatChoice choice = mock(ChatChoice.class);
        when(choice.getMessage()).thenReturn(responseMessage);

        ChatCompletions completions = mock(ChatCompletions.class);
        when(completions.getChoices()).thenReturn(List.of(choice));

        ChatCompletionsClient mockClient = mock(ChatCompletionsClient.class);
        when(mockClient.complete(any(ChatCompletionsOptions.class))).thenReturn(completions);

        try (MockedConstruction<ChatCompletionsClientBuilder> ignored = Mockito.mockConstruction(ChatCompletionsClientBuilder.class, (mock, ctx) ->
        {
            when(mock.endpoint(anyString())).thenReturn(mock);
            when(mock.credential(any(com.azure.core.credential.TokenCredential.class))).thenReturn(mock);
            when(mock.credential(any(com.azure.core.credential.KeyCredential.class))).thenReturn(mock);
            when(mock.buildClient()).thenReturn(mockClient);
        })) {

            ChatCompletion.Output output = task.run(runContext);

            ChatCompletionsClientBuilder builderMock = ignored.constructed().get(0);
            verify(builderMock).credential(any(com.azure.core.credential.KeyCredential.class));

            assertThat(output, notNullValue());
            assertThat(output.getContent(), is("Hello from Azure!"));

            ArgumentCaptor<ChatCompletionsOptions> captor = ArgumentCaptor.forClass(ChatCompletionsOptions.class);
            verify(mockClient).complete(captor.capture());

            ChatCompletionsOptions options = captor.getValue();
            assertThat(options.getModel(), is("gpt-4o"));
            assertThat(options.getMessages().size(), is(1));

            ChatRequestMessage sentMessage = options.getMessages().get(0);
            assertThat(sentMessage instanceof ChatRequestUserMessage, is(true));
        }
    }
}
