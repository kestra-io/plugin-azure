package io.kestra.plugin.azure.storage.adls;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

import com.azure.core.http.rest.PagedFlux;
import com.azure.core.http.rest.PagedResponse;
import com.azure.storage.file.datalake.DataLakeFileSystemAsyncClient;
import com.azure.storage.file.datalake.DataLakeServiceAsyncClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.models.PathItem;

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
}
