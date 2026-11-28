package io.kestra.plugin.azure.storage.blob;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import com.azure.core.http.HttpHeaders;
import com.azure.core.http.rest.PagedFlux;
import com.azure.core.http.rest.PagedResponse;
import com.azure.core.http.rest.PagedResponseBase;
import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobItemProperties;
import com.azure.storage.blob.models.BlobProperties;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.azure.storage.blob.abstracts.ActionInterface;
import io.kestra.plugin.azure.storage.blob.services.BlobService;

import jakarta.inject.Inject;
import reactor.core.publisher.Mono;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.mockito.Mockito.*;

@KestraTest
class TriggerMaxFilesTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void blobsBeyondMaxFilesFireOnLaterPolls() throws Exception {
        var items = Stream.of("a.csv", "b.csv", "c.csv")
            .map(name -> new BlobItem().setName(name).setProperties(new BlobItemProperties().setContentType("text/csv").setETag("v1")))
            .toList();
        var asyncClient = mock(BlobServiceAsyncClient.class);
        var containerClient = mock(BlobContainerAsyncClient.class);
        var blobClient = mock(BlobAsyncClient.class);
        var props = mock(BlobProperties.class);
        when(props.getBlobSize()).thenReturn(1L);
        when(asyncClient.getBlobContainerAsyncClient(anyString())).thenReturn(containerClient);
        when(containerClient.listBlobs(any())).thenAnswer(
            invocation -> new PagedFlux<>(() -> Mono.just(new PagedResponseBase<Void, BlobItem>(null, 200, new HttpHeaders(), items, null, null)))
        );
        when(containerClient.getBlobAsyncClient(anyString())).thenReturn(blobClient);
        when(blobClient.downloadToFile(anyString(), eq(true))).thenReturn(Mono.just(props));

        try (var mocked = mockStatic(BlobService.class)) {
            mocked.when(() -> BlobService.asyncClient(any(), any(), any(), any(), any(), any())).thenReturn(asyncClient);
            var trigger = Trigger.builder()
                .id("blob-" + IdUtils.create())
                .type(Trigger.class.getName())
                .connectionString(Property.ofValue("DefaultEndpointsProtocol=https;AccountName=dummy;AccountKey=dummy;EndpointSuffix=core.windows.net"))
                .container(Property.ofValue("container"))
                .action(Property.ofValue(ActionInterface.Action.NONE))
                .maxFiles(Property.ofValue(1))
                .interval(Duration.ofSeconds(10))
                .build();
            var context = TestsUtils.mockTrigger(runContextFactory, trigger);

            var fired = new ArrayList<Object>();
            for (int poll = 0; poll < 4; poll++) {
                var execution = trigger.evaluate(context.getKey(), context.getValue().context());
                fired.add(execution.map(e -> ((List<?>) e.getTrigger().getVariables().get("blobs")).size()).orElse(0));
            }

            assertThat(fired, contains(1, 1, 1, 0));
            for (var name : List.of("a.csv", "b.csv", "c.csv")) {
                verify(containerClient).getBlobAsyncClient(name);
            }
        }
    }
}
