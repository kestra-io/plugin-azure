package io.kestra.plugin.azure.aifoundry;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;

@KestraTest
class ChatCompletionTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    @Disabled("Needs Azure AI Foundry credentials")
    void run() throws Exception {
        ChatCompletion task = ChatCompletion.builder()
            .id("chat-completion")
            .type(ChatCompletion.class.getName())
            .endpoint(Property.ofValue("https://your-endpoint.openai.azure.com/"))
            .apiKey(Property.ofValue("your-api-key"))
            .deploymentName(Property.ofValue("gpt-4o"))
            .messages(
                Property.ofValue(
                    List.of(
                        ChatCompletion.ChatMessage.builder()
                            .role(Property.ofValue("user"))
                            .content(Property.ofValue("Hello, world!"))
                            .build()
                    )
                )
            )
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        ChatCompletion.Output runOutput = task.run(runContext);

        assertThat(runOutput.getContent(), notNullValue());
    }
}
