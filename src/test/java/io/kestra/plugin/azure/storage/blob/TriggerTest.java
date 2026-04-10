package io.kestra.plugin.azure.storage.blob;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import io.kestra.plugin.azure.shared.storage.blob.models.Blob;
import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.StatefulTriggerInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.azure.storage.blob.abstracts.ActionInterface;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class TriggerTest extends AbstractTest {

    @Test
    void deleteAction() throws Exception {
        String toUploadDir = "trigger/storage-listen";
        upload(toUploadDir);
        upload(toUploadDir);

        Trigger trigger = Trigger.builder()
            .id("blob-delete-" + IdUtils.create())
            .type(Trigger.class.getName())
            .endpoint(Property.ofValue(storageEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .container(Property.ofValue(container))
            .prefix(Property.ofValue(toUploadDir))
            .action(Property.ofValue(ActionInterface.Action.DELETE))
            .interval(Duration.ofSeconds(10))
            .build();

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = TestsUtils.mockTrigger(runContextFactory, trigger);
        Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());

        assertThat(execution.isPresent(), is(true));

        @SuppressWarnings("unchecked")
        java.util.List<Blob> blobs = (java.util.List<Blob>) execution.get().getTrigger().getVariables().get("blobs");
        assertThat(blobs.size(), is(2));

        List listTask = list()
            .prefix(Property.ofValue(toUploadDir))
            .build();
        int remainingFilesOnBucket = listTask.run(runContext(listTask))
            .getBlobs()
            .size();
        assertThat(remainingFilesOnBucket, is(0));
    }

    @Test
    void noneAction() throws Exception {
        String toUploadDir = "trigger/none-action-storage-listen";
        try {
            upload(toUploadDir);
            upload(toUploadDir);

            Trigger trigger = Trigger.builder()
                .id("blob-none-" + IdUtils.create())
                .type(Trigger.class.getName())
                .endpoint(Property.ofValue(storageEndpoint))
                .connectionString(Property.ofValue(connectionString))
                .container(Property.ofValue(container))
                .prefix(Property.ofValue(toUploadDir))
                .action(Property.ofValue(ActionInterface.Action.NONE))
                .interval(Duration.ofSeconds(10))
                .build();

            Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = TestsUtils.mockTrigger(runContextFactory, trigger);
            Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());

            assertThat(execution.isPresent(), is(true));

            @SuppressWarnings("unchecked")
            java.util.List<Blob> blobs = (java.util.List<Blob>) execution.get().getTrigger().getVariables().get("blobs");
            assertThat(blobs.size(), is(2));

            List listTask = list()
                .prefix(Property.ofValue(toUploadDir))
                .build();
            int remainingFilesOnBucket = listTask.run(runContext(listTask))
                .getBlobs()
                .size();
            assertThat(remainingFilesOnBucket, is(2));
        } finally {
            DeleteList cleaner = deleteDir(toUploadDir).build();
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
}
