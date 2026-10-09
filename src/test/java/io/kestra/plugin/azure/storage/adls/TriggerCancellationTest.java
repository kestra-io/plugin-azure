package io.kestra.plugin.azure.storage.adls;

import java.time.Duration;
import java.time.OffsetDateTime;
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
import com.azure.storage.file.datalake.DataLakeFileAsyncClient;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemAsyncClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceAsyncClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.models.PathItem;
import com.azure.storage.file.datalake.models.PathProperties;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.azure.storage.adls.services.DataLakeService;

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
        var asyncClient = mock(DataLakeServiceAsyncClient.class);
        var fileSystemClient = mock(DataLakeFileSystemAsyncClient.class);
        when(asyncClient.getFileSystemAsyncClient(anyString())).thenReturn(fileSystemClient);
        when(fileSystemClient.listPaths(any())).thenReturn(
            new PagedFlux<>(
                () -> Mono.<PagedResponse<PathItem>> never()
                    .doOnSubscribe(s -> subscribed.countDown())
                    .doOnCancel(() -> cancelled.set(true))
            )
        );

        try (var mocked = mockStatic(DataLakeService.class)) {
            mocked.when(() -> DataLakeService.asyncClient(any(), any(), any(), any(), any(), any())).thenReturn(asyncClient);
            mocked.when(() -> DataLakeService.client(any(), any(), any(), any(), any(), any()))
                .thenReturn(mock(DataLakeServiceClient.class, RETURNS_MOCKS));

            var trigger = Trigger.builder()
                .id("adls-" + IdUtils.create())
                .type(Trigger.class.getName())
                .connectionString(Property.ofValue("DefaultEndpointsProtocol=https;AccountName=dummy;AccountKey=dummy;EndpointSuffix=core.windows.net"))
                .fileSystem(Property.ofValue("container"))
                .directoryPath(Property.ofValue("trigger/adls/cancel"))
                .action(Property.ofValue(Trigger.Action.NONE))
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
            .id("adls-" + IdUtils.create())
            .type(Trigger.class.getName())
            .connectionString(Property.ofValue("DefaultEndpointsProtocol=https;AccountName=dummy;AccountKey=dummy;EndpointSuffix=core.windows.net"))
            .fileSystem(Property.ofValue("container"))
            .directoryPath(Property.ofValue("trigger/adls/cancel"))
            .action(Property.ofValue(Trigger.Action.NONE))
            .interval(Duration.ofSeconds(10));
    }

    private static DataLakeServiceClient syncClientWithOneFile() {
        var now = OffsetDateTime.now();
        var props = mock(PathProperties.class);
        when(props.getCreationTime()).thenReturn(now);
        when(props.getLastModified()).thenReturn(now);
        when(props.getETag()).thenReturn("etag");
        when(props.getAccessControlList()).thenReturn(List.of());
        var fileClient = mock(DataLakeFileClient.class);
        when(fileClient.getProperties()).thenReturn(props);
        when(fileClient.getFilePath()).thenReturn("trigger/adls/cancel/file.csv");
        when(fileClient.getFileName()).thenReturn("file.csv");
        var fsClient = mock(DataLakeFileSystemClient.class);
        when(fsClient.getFileClient(anyString())).thenReturn(fileClient);
        var serviceClient = mock(DataLakeServiceClient.class);
        when(serviceClient.getFileSystemClient(anyString())).thenReturn(fsClient);
        return serviceClient;
    }

    private static DataLakeServiceAsyncClient asyncClientWithOneFile(Mono<PathProperties> download) {
        var item = mock(PathItem.class);
        when(item.getName()).thenReturn("trigger/adls/cancel/file.csv");
        var fileClient = mock(DataLakeFileAsyncClient.class);
        when(fileClient.readToFile(anyString(), eq(true))).thenReturn(download);
        var fsClient = mock(DataLakeFileSystemAsyncClient.class);
        when(fsClient.listPaths(any())).thenReturn(
            new PagedFlux<>(() -> Mono.just(new PagedResponseBase<Void, PathItem>(null, 200, new HttpHeaders(), List.of(item), null, null)))
        );
        when(fsClient.getFileAsyncClient(anyString())).thenReturn(fileClient);
        var asyncClient = mock(DataLakeServiceAsyncClient.class);
        when(asyncClient.getFileSystemAsyncClient(anyString())).thenReturn(fsClient);
        return asyncClient;
    }

    @Test
    void downloadIsNotBoundedByListingTimeout() throws Exception {
        // Completes well after the (shortened) listing timeout.
        var asyncClient = asyncClientWithOneFile(Mono.delay(Duration.ofMillis(800)).thenReturn(mock(PathProperties.class)));

        var syncClient = syncClientWithOneFile();

        try (var mocked = mockStatic(DataLakeService.class)) {
            mocked.when(() -> DataLakeService.asyncClient(any(), any(), any(), any(), any(), any())).thenReturn(asyncClient);
            mocked.when(() -> DataLakeService.client(any(), any(), any(), any(), any(), any())).thenReturn(syncClient);
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
        var asyncClient = asyncClientWithOneFile(
            Mono.<PathProperties> never()
                .doOnSubscribe(s -> subscribed.countDown())
                .doOnCancel(() -> cancelled.set(true))
        );

        var syncClient = syncClientWithOneFile();

        try (var mocked = mockStatic(DataLakeService.class)) {
            mocked.when(() -> DataLakeService.asyncClient(any(), any(), any(), any(), any(), any())).thenReturn(asyncClient);
            mocked.when(() -> DataLakeService.client(any(), any(), any(), any(), any(), any())).thenReturn(syncClient);
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
