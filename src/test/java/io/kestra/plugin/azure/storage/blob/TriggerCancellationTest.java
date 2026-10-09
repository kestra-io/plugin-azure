package io.kestra.plugin.azure.storage.blob;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.mockito.Mockito.*;

@KestraTest
class TriggerCancellationTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void killDuringEvaluate() throws Exception {
        var subscribed = new CountDownLatch(1);
        var cancelled = new AtomicBoolean();
        var asyncClient = mock(BlobServiceAsyncClient.class);
        var containerClient = mock(BlobContainerAsyncClient.class);
        when(asyncClient.getBlobContainerAsyncClient(anyString())).thenReturn(containerClient);
        when(containerClient.listBlobs(any())).thenReturn(
            new PagedFlux<>(
                () -> Mono.<PagedResponse<BlobItem>> never()
                    .doOnSubscribe(s -> subscribed.countDown())
                    .doOnCancel(() -> cancelled.set(true))
            )
        );

        try (var mocked = mockStatic(BlobService.class)) {
            mocked.when(() -> BlobService.asyncClient(any(), any(), any(), any(), any(), any())).thenReturn(asyncClient);
            var trigger = Trigger.builder()
                .id("blob-" + IdUtils.create())
                .type(Trigger.class.getName())
                .connectionString(Property.ofValue("DefaultEndpointsProtocol=https;AccountName=dummy;AccountKey=dummy;EndpointSuffix=core.windows.net"))
                .container(Property.ofValue("container"))
                .action(Property.ofValue(ActionInterface.Action.NONE))
                .interval(Duration.ofSeconds(10))
                .build();
            var context = TestsUtils.mockTrigger(runContextFactory, trigger);

            var killer = CompletableFuture.runAsync(() ->
            {
                try {
                    if (subscribed.await(5, TimeUnit.SECONDS)) {
                        trigger.kill();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            var execution = trigger.evaluate(context.getKey(), context.getValue().context());
            killer.get(5, TimeUnit.SECONDS);

            assertThat(execution.isEmpty(), is(true));
            assertThat(cancelled.get(), is(true));
        }
    }

    private static Trigger.TriggerBuilder<?, ?> triggerBuilder() {
        return Trigger.builder()
            .id("blob-" + IdUtils.create())
            .type(Trigger.class.getName())
            .connectionString(Property.ofValue("DefaultEndpointsProtocol=https;AccountName=dummy;AccountKey=dummy;EndpointSuffix=core.windows.net"))
            .container(Property.ofValue("container"))
            .action(Property.ofValue(ActionInterface.Action.NONE))
            .interval(Duration.ofSeconds(10));
    }

    private static PagedFlux<BlobItem> singleBlob() {
        var item = new BlobItem()
            .setName("dir/file.csv")
            .setProperties(new BlobItemProperties().setContentType("text/csv"));
        return new PagedFlux<>(() -> Mono.just(new PagedResponseBase<Void, BlobItem>(null, 200, new HttpHeaders(), List.of(item), null, null)));
    }

    private void mockedDownload(Mono<BlobProperties> download, BlobServiceAsyncClient asyncClient) {
        var containerClient = mock(BlobContainerAsyncClient.class);
        var blobClient = mock(BlobAsyncClient.class);
        when(asyncClient.getBlobContainerAsyncClient(anyString())).thenReturn(containerClient);
        when(containerClient.listBlobs(any())).thenReturn(singleBlob());
        when(containerClient.getBlobAsyncClient(anyString())).thenReturn(blobClient);
        when(blobClient.downloadToFile(anyString(), eq(true))).thenReturn(download);
    }

    @Test
    void downloadIsNotBoundedByListingTimeout() throws Exception {
        var asyncClient = mock(BlobServiceAsyncClient.class);
        var props = mock(BlobProperties.class);
        when(props.getBlobSize()).thenReturn(42L);
        // Completes well after the (shortened) listing timeout.
        mockedDownload(Mono.delay(Duration.ofMillis(800)).thenReturn(props), asyncClient);

        try (var mocked = mockStatic(BlobService.class)) {
            mocked.when(() -> BlobService.asyncClient(any(), any(), any(), any(), any(), any())).thenReturn(asyncClient);
            var trigger = triggerBuilder().listTimeout(Duration.ofMillis(200)).build();
            var context = TestsUtils.mockTrigger(runContextFactory, trigger);

            var execution = trigger.evaluate(context.getKey(), context.getValue().context());

            assertThat(execution.isPresent(), is(true));
        }
    }

    @Test
    void killDuringDownload() throws Exception {
        var subscribed = new CountDownLatch(1);
        var cancelled = new AtomicBoolean();
        var asyncClient = mock(BlobServiceAsyncClient.class);
        mockedDownload(
            Mono.<BlobProperties> never()
                .doOnSubscribe(s -> subscribed.countDown())
                .doOnCancel(() -> cancelled.set(true)),
            asyncClient
        );

        try (var mocked = mockStatic(BlobService.class)) {
            mocked.when(() -> BlobService.asyncClient(any(), any(), any(), any(), any(), any())).thenReturn(asyncClient);
            var trigger = triggerBuilder().build();
            var context = TestsUtils.mockTrigger(runContextFactory, trigger);

            var killer = CompletableFuture.runAsync(() ->
            {
                try {
                    if (subscribed.await(5, TimeUnit.SECONDS)) {
                        trigger.kill();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            var start = System.nanoTime();
            var execution = trigger.evaluate(context.getKey(), context.getValue().context());
            killer.get(5, TimeUnit.SECONDS);

            assertThat(execution.isEmpty(), is(true));
            assertThat(cancelled.get(), is(true));
            assertThat(Duration.ofNanos(System.nanoTime() - start).toSeconds(), lessThan(5L));
        }
    }
}
