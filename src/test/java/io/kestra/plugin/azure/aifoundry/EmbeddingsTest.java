package io.kestra.plugin.azure.aifoundry;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import com.azure.ai.inference.EmbeddingsClient;
import com.azure.ai.inference.EmbeddingsClientBuilder;
import com.azure.ai.inference.models.EmbeddingItem;
import com.azure.ai.inference.models.EmbeddingsResult;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@KestraTest
class EmbeddingsTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run_withMockedClient_returnsEmbeddingsAndVerifiesArgs() throws Exception {
        Embeddings task = Embeddings.builder()
            .id("embeddings")
            .type(Embeddings.class.getName())
            .endpoint(Property.ofValue("https://test.api.azureml.ms/"))
            .apiKey(Property.ofValue("test-key"))
            .deploymentName(Property.ofValue("text-embedding-3-small"))
            .inputs(Property.ofValue(List.of("First input text", "Second input text")))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        EmbeddingItem item1 = mock(EmbeddingItem.class);
        when(item1.getEmbeddingList()).thenReturn(List.of(0.1f, 0.2f, 0.3f));

        EmbeddingItem item2 = mock(EmbeddingItem.class);
        when(item2.getEmbeddingList()).thenReturn(List.of(0.4f, 0.5f, 0.6f));

        EmbeddingsResult embeddingsResult = mock(EmbeddingsResult.class);
        when(embeddingsResult.getData()).thenReturn(List.of(item1, item2));

        EmbeddingsClient mockClient = mock(EmbeddingsClient.class);
        when(mockClient.embed(any(java.util.List.class), any(), any(), any(), nullable(String.class), any())).thenReturn(embeddingsResult);

        try (MockedConstruction<EmbeddingsClientBuilder> ignored = Mockito.mockConstruction(EmbeddingsClientBuilder.class, (mock, ctx) ->
        {
            when(mock.endpoint(anyString())).thenReturn(mock);
            when(mock.credential(any(com.azure.core.credential.TokenCredential.class))).thenReturn(mock);
            when(mock.credential(any(com.azure.core.credential.KeyCredential.class))).thenReturn(mock);
            when(mock.buildClient()).thenReturn(mockClient);
        })) {

            Embeddings.Output output = task.run(runContext);

            EmbeddingsClientBuilder builderMock = ignored.constructed().get(0);
            verify(builderMock).credential(any(com.azure.core.credential.KeyCredential.class));

            assertThat(output, notNullValue());
            assertThat(output.getEmbeddings().size(), is(2));
            assertThat(output.getEmbeddings().get(0), contains(0.1f, 0.2f, 0.3f));
            assertThat(output.getEmbeddings().get(1), contains(0.4f, 0.5f, 0.6f));

            ArgumentCaptor<java.util.List<String>> captor = ArgumentCaptor.forClass(java.util.List.class);
            verify(mockClient).embed(captor.capture(), any(), any(), any(), nullable(String.class), any());

            assertThat(captor.getValue().size(), is(2));
            assertThat(captor.getValue(), contains("First input text", "Second input text"));
        }
    }
}
