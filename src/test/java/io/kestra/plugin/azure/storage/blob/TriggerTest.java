package io.kestra.plugin.azure.storage.blob;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.StatefulTriggerInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.azure.shared.storage.blob.models.Blob;
import io.kestra.plugin.azure.storage.blob.abstracts.ActionInterface;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class TriggerTest extends AbstractTest {
    @Test
    void deleteAction() throws Exception {
        String prefix = "trigger/storage-listen";

        Trigger trigger = Trigger.builder()
            .id("blob-" + IdUtils.create())
            .type(Trigger.class.getName())
            .endpoint(Property.ofValue(storageEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .container(Property.ofValue(container))
            .prefix(Property.ofValue(prefix))
            .action(Property.ofValue(ActionInterface.Action.DELETE))
            .on(Property.ofValue(StatefulTriggerInterface.On.CREATE))
            .interval(Duration.ofSeconds(10))
            .build();

        upload(prefix);
        upload(prefix);

        Map.Entry<ConditionContext, io.kestra.core.scheduler.model.TriggerState> context = TestsUtils.mockTrigger(runContextFactory, trigger);
        Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue().context());

        assertThat(execution.isPresent(), is(true));

        @SuppressWarnings("unchecked")
        java.util.List<Blob> blobs = (java.util.List<Blob>) execution.get().getTrigger().getVariables().get("blobs");
        assertThat(blobs.size(), is(2));

        // action DELETE must have removed everything it matched
        List listTask = list()
            .prefix(Property.ofValue(prefix))
            .build();
        int remainingFilesOnBucket = listTask.run(runContext(listTask))
            .getBlobs()
            .size();
        assertThat(remainingFilesOnBucket, is(0));
    }

    @Test
    void noneAction() throws Exception {
        String prefix = "trigger/none-action-storage-listen";

        Trigger trigger = Trigger.builder()
            .id("blob-" + IdUtils.create())
            .type(Trigger.class.getName())
            .endpoint(Property.ofValue(storageEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .container(Property.ofValue(container))
            .prefix(Property.ofValue(prefix))
            .action(Property.ofValue(ActionInterface.Action.NONE))
            .on(Property.ofValue(StatefulTriggerInterface.On.CREATE))
            .interval(Duration.ofSeconds(10))
            .build();

        try {
            upload(prefix);
            upload(prefix);

            Map.Entry<ConditionContext, io.kestra.core.scheduler.model.TriggerState> context = TestsUtils.mockTrigger(runContextFactory, trigger);
            Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue().context());

            assertThat(execution.isPresent(), is(true));

            @SuppressWarnings("unchecked")
            java.util.List<Blob> blobs = (java.util.List<Blob>) execution.get().getTrigger().getVariables().get("blobs");
            assertThat(blobs.size(), is(2));

            // action NONE must leave the blobs in place
            List listTask = list()
                .prefix(Property.ofValue(prefix))
                .build();
            int remainingFilesOnBucket = listTask.run(runContext(listTask))
                .getBlobs()
                .size();
            assertThat(remainingFilesOnBucket, is(2));
        } finally {
            DeleteList cleaner = deleteDir(prefix).build();
            cleaner.run(runContext(cleaner));
        }
    }

    @Test
    void shouldExecuteOnCreate() throws Exception {
        Trigger trigger = Trigger.builder()
            .id("blob-" + IdUtils.create())
            .type(Trigger.class.getName())
            .endpoint(Property.ofValue(storageEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .container(Property.ofValue(container))
            .prefix(Property.ofValue("trigger/blob/on-create"))
            .action(Property.ofValue(ActionInterface.Action.NONE))
            .on(Property.ofValue(StatefulTriggerInterface.On.CREATE))
            .interval(Duration.ofSeconds(10))
            .build();

        upload("trigger/blob/on-create");

        Map.Entry<ConditionContext, io.kestra.core.scheduler.model.TriggerState> context = TestsUtils.mockTrigger(runContextFactory, trigger);
        Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue().context());

        assertThat(execution.isPresent(), is(true));
    }

    @Test
    void shouldExecuteOnUpdate() throws Exception {
        var output = upload("trigger/blob/on-update");

        Trigger trigger = Trigger.builder()
            .id("blob-" + IdUtils.create())
            .type(Trigger.class.getName())
            .endpoint(Property.ofValue(storageEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .container(Property.ofValue(container))
            .prefix(Property.ofValue("trigger/blob/on-update"))
            .action(Property.ofValue(ActionInterface.Action.NONE))
            .on(Property.ofValue(StatefulTriggerInterface.On.UPDATE))
            .interval(Duration.ofSeconds(10))
            .build();

        Map.Entry<ConditionContext, io.kestra.core.scheduler.model.TriggerState> context = TestsUtils.mockTrigger(runContextFactory, trigger);

        trigger.evaluate(context.getKey(), context.getValue().context());

        update(output.getBlob().getName());
        Thread.sleep(2000);

        Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue().context());
        assertThat(execution.isPresent(), is(true));
    }

    @Test
    void shouldExecuteOnCreateOrUpdate() throws Exception {
        Trigger trigger = Trigger.builder()
            .id("blob-" + IdUtils.create())
            .type(Trigger.class.getName())
            .endpoint(Property.ofValue(storageEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .container(Property.ofValue(container))
            .prefix(Property.ofValue("trigger/blob/on-create-or-update"))
            .action(Property.ofValue(ActionInterface.Action.NONE))
            .interval(Duration.ofSeconds(10))
            .build();

        var output = upload("trigger/blob/on-create-or-update/");

        Map.Entry<ConditionContext, io.kestra.core.scheduler.model.TriggerState> context = TestsUtils.mockTrigger(runContextFactory, trigger);

        Optional<Execution> createExecution = trigger.evaluate(context.getKey(), context.getValue().context());
        assertThat(createExecution.isPresent(), is(true));

        update(output.getBlob().getName());
        Thread.sleep(2000);

        Optional<Execution> updateExecution = trigger.evaluate(context.getKey(), context.getValue().context());
        assertThat(updateExecution.isPresent(), is(true));
    }

    @Test
    void testCancellation() throws Exception {
        Trigger trigger = Trigger.builder()
            .id("blob-" + IdUtils.create())
            .type(Trigger.class.getName())
            .endpoint(Property.ofValue(storageEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .container(Property.ofValue(container))
            .prefix(Property.ofValue("trigger/blob/cancel"))
            .action(Property.ofValue(ActionInterface.Action.NONE))
            .interval(Duration.ofSeconds(10))
            .build();

        var asyncClientMock = org.mockito.Mockito.mock(com.azure.storage.blob.BlobServiceAsyncClient.class);
        var containerMock = org.mockito.Mockito.mock(com.azure.storage.blob.BlobContainerAsyncClient.class);
        org.mockito.Mockito.when(asyncClientMock.getBlobContainerAsyncClient(org.mockito.Mockito.anyString())).thenReturn(containerMock);
        org.mockito.Mockito.when(containerMock.listBlobs(org.mockito.Mockito.any())).thenReturn(reactor.core.publisher.Flux.never());

        try (
            org.mockito.MockedStatic<io.kestra.plugin.azure.storage.blob.services.BlobService> mockedStatic = org.mockito.Mockito
                .mockStatic(io.kestra.plugin.azure.storage.blob.services.BlobService.class)
        ) {
            mockedStatic.when(
                () -> io.kestra.plugin.azure.storage.blob.services.BlobService.asyncClient(
                    org.mockito.Mockito.any(), org.mockito.Mockito.any(), org.mockito.Mockito.any(), org.mockito.Mockito.any(), org.mockito.Mockito.any(), org.mockito.Mockito.any()
                )
            ).thenReturn(asyncClientMock);

            Map.Entry<ConditionContext, io.kestra.core.scheduler.model.TriggerState> context = TestsUtils.mockTrigger(runContextFactory, trigger);

            // Test 1: kill before evaluate
            trigger.kill();
            Optional<Execution> executionBefore = trigger.evaluate(context.getKey(), context.getValue().context());
            assertThat(executionBefore.isEmpty(), is(true));

            // Test 2: kill during evaluate
            Trigger trigger2 = trigger.toBuilder().id("blob-" + IdUtils.create()).build();
            Map.Entry<ConditionContext, io.kestra.core.scheduler.model.TriggerState> context2 = TestsUtils.mockTrigger(runContextFactory, trigger2);

            java.util.concurrent.atomic.AtomicReference<Optional<Execution>> result = new java.util.concurrent.atomic.AtomicReference<>();
            java.util.concurrent.atomic.AtomicReference<Exception> error = new java.util.concurrent.atomic.AtomicReference<>();

            Thread t = new Thread(() ->
            {
                try {
                    result.set(trigger2.evaluate(context2.getKey(), context2.getValue().context()));
                } catch (Exception e) {
                    error.set(e);
                }
            });

            t.start();
            Thread.sleep(100);
            trigger2.kill();
            t.join(2000);

            assertThat(t.isAlive(), is(false));
            if (error.get() != null)
                throw error.get();
            assertThat(result.get().isEmpty(), is(true));
        }
    }
}
