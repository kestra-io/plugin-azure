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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@KestraTest
class EmbeddingsTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    @Disabled("Needs Azure AI Foundry credentials")
    void run() throws Exception {
        Embeddings task = Embeddings.builder()
            .id("embeddings")
            .type(Embeddings.class.getName())
            .endpoint(Property.ofValue("https://your-endpoint.openai.azure.com/"))
            .apiKey(Property.ofValue("your-api-key"))
            .deploymentName(Property.ofValue("text-embedding-3-small"))
            .inputs(Property.ofValue(List.of("The quick brown fox jumps over the lazy dog.")))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        Embeddings.Output runOutput = task.run(runContext);

        assertThat(runOutput.getEmbeddings().size(), is(1));
        assertThat(runOutput.getEmbeddings().get(0), notNullValue());
    }
}
