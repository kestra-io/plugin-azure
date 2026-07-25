package io.kestra.plugin.azure.storage.adls;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.StatefulTriggerInterface;
import io.kestra.core.queues.DispatchQueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.azure.storage.adls.models.AdlsFile;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest(startRunner = true, startScheduler = true)
@Disabled("Unit tests works correctly locally but fail on the CI - temporary disable them")
class TriggerTest extends AbstractTest {
    @Inject
    private DispatchQueueInterface<Execution> executionQueue;

    @Inject
    protected LocalFlowRepositoryLoader repositoryLoader;

    @Test
    void trigger() throws Exception {
        CountDownLatch queueCount = new CountDownLatch(1);
        AtomicReference<Execution> last = new AtomicReference<>();

        executionQueue.addListener(executionWithError ->
        {
            if (executionWithError.getFlowId().equals("adls-listen")) {
                last.set(executionWithError);
                queueCount.countDown();
            }
        });

        try {
            upload("adls/azure/trigger/adls-listen");
            upload("adls/azure/trigger/adls-listen");

            repositoryLoader.load(Objects.requireNonNull(TriggerTest.class.getClassLoader().getResource("flows/adls-listen.yaml")));

            boolean await = queueCount.await(10, TimeUnit.SECONDS);
            assertThat(await, is(true));

            @SuppressWarnings("unchecked")
            java.util.List<AdlsFile> trigger = (java.util.List<AdlsFile>) last.get().getTrigger().getVariables().get("files");

            assertThat(trigger.size(), is(2));

            List listTask = list()
                .directoryPath(Property.ofValue("adls/azure/trigger/adls-listen"))
                .build();
            int remainingFilesOnBucket = listTask.run(runContext(listTask))
                .getFiles()
                .size();
            assertThat(remainingFilesOnBucket, is(2));
        } finally {
            DeleteFiles cleaner = deleteDir("adls/azure/trigger/adls-listen").build();
            cleaner.run(runContext(cleaner));
        }
    }

    @Test
    void deleteAction() throws Exception {
        CountDownLatch queueCount = new CountDownLatch(1);
        AtomicReference<Execution> last = new AtomicReference<>();

        executionQueue.addListener(executionWithError ->
        {
            if (executionWithError.getFlowId().equals("adls-listen-delete-action")) {
                last.set(executionWithError);
                queueCount.countDown();
            }
        });

        upload("adls/azure/trigger/adls-listen-delete-action");
        upload("adls/azure/trigger/adls-listen-delete-action");

        repositoryLoader.load(Objects.requireNonNull(TriggerTest.class.getClassLoader().getResource("flows/adls-listen-delete-action.yaml")));

        boolean await = queueCount.await(10, TimeUnit.SECONDS);
        assertThat(await, is(true));

        @SuppressWarnings("unchecked")
        java.util.List<AdlsFile> trigger = (java.util.List<AdlsFile>) last.get().getTrigger().getVariables().get("files");

        assertThat(trigger.size(), is(2));

        List listTask = list()
            .directoryPath(Property.ofValue("adls/azure/trigger/adls-listen-delete-action"))
            .build();

        int remainingFilesOnBucket = listTask.run(runContext(listTask))
            .getFiles()
            .size();
        assertThat(remainingFilesOnBucket, is(0));
    }

    @Test
    void moveAction() throws Exception {
        CountDownLatch queueCount = new CountDownLatch(1);
        AtomicReference<Execution> last = new AtomicReference<>();

        executionQueue.addListener(executionWithError ->
        {
            if (executionWithError.getFlowId().equals("adls-listen-move-action")) {
                last.set(executionWithError);
                queueCount.countDown();
            }
        });

        try {
            upload("adls/azure/trigger/adls-listen-move-action");
            upload("adls/azure/trigger/adls-listen-move-action");

            repositoryLoader.load(Objects.requireNonNull(TriggerTest.class.getClassLoader().getResource("flows/adls-listen-move-action.yaml")));

            boolean await = queueCount.await(10, TimeUnit.SECONDS);
            assertThat(await, is(true));

            @SuppressWarnings("unchecked")
            java.util.List<AdlsFile> trigger = (java.util.List<AdlsFile>) last.get().getTrigger().getVariables().get("files");

            assertThat(trigger.size(), is(2));

            List listTask = list()
                .directoryPath(Property.ofValue("adls/azure/trigger/adls-listen-move-action-direction"))
                .build();

            int movedFilesOnBucket = listTask.run(runContext(listTask))
                .getFiles()
                .size();
            assertThat(movedFilesOnBucket, is(2));

            List remainingFiles = list()
                .directoryPath(Property.ofValue("adls/azure/trigger/adls-listen-move-action"))
                .build();

            int remainingFilesOnBucket = remainingFiles.run(runContext(listTask))
                .getFiles()
                .size();
            assertThat(remainingFilesOnBucket, is(0));
        } finally {
            DeleteFiles cleaner = deleteDir("adls/azure/trigger/adls-listen-move-action-direction").build();
            cleaner.run(runContext(cleaner));

            cleaner = deleteDir("adls/azure/trigger/adls-listen-move-action").build();
            cleaner.run(runContext(cleaner));
        }
    }

    @Test
    void shouldExecuteOnCreate() throws Exception {
        Trigger trigger = Trigger.builder()
            .id("adls-" + IdUtils.create())
            .type(Trigger.class.getName())
            .endpoint(Property.ofValue(this.adlsEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .fileSystem(Property.ofValue(fileSystem))
            .directoryPath(Property.ofValue("trigger/adls/on-create"))
            .action(Property.ofValue(Trigger.Action.NONE))
            .on(Property.ofValue(StatefulTriggerInterface.On.CREATE))
            .interval(Duration.ofSeconds(10))
            .build();

        upload("adls/trigger/adls/on-create");

        Map.Entry<ConditionContext, io.kestra.core.scheduler.model.TriggerState> context = TestsUtils.mockTrigger(runContextFactory, trigger);
        Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue().context());

        assertThat(execution.isPresent(), is(true));

        DeleteFiles cleaner = deleteDir("trigger/adls/on-create").build();
        cleaner.run(runContext(cleaner));
    }

    @Test
    void shouldExecuteOnUpdate() throws Exception {
        upload("adls/trigger/adls/on-update");

        Trigger trigger = Trigger.builder()
            .id("adls-" + IdUtils.create())
            .type(Trigger.class.getName())
            .endpoint(Property.ofValue(this.adlsEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .fileSystem(Property.ofValue(fileSystem))
            .directoryPath(Property.ofValue("trigger/adls/on-update"))
            .action(Property.ofValue(Trigger.Action.NONE))
            .on(Property.ofValue(StatefulTriggerInterface.On.UPDATE))
            .interval(Duration.ofSeconds(10))
            .build();

        Map.Entry<ConditionContext, io.kestra.core.scheduler.model.TriggerState> context = TestsUtils.mockTrigger(runContextFactory, trigger);

        trigger.evaluate(context.getKey(), context.getValue().context());

        update("adls/trigger/adls/on-update");
        Thread.sleep(2000);

        Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue().context());
        assertThat(execution.isPresent(), is(true));

        DeleteFiles cleaner = deleteDir("trigger/adls/on-update").build();
        cleaner.run(runContext(cleaner));
    }

    @Test
    void shouldExecuteOnCreateOrUpdate() throws Exception {
        Trigger trigger = Trigger.builder()
            .id("adls-" + IdUtils.create())
            .type(Trigger.class.getName())
            .endpoint(Property.ofValue(this.adlsEndpoint))
            .connectionString(Property.ofValue(connectionString))
            .fileSystem(Property.ofValue(fileSystem))
            .directoryPath(Property.ofValue("trigger/adls/on-create-or-update"))
            .action(Property.ofValue(Trigger.Action.NONE))
            .interval(Duration.ofSeconds(10))
            .build();

        upload("trigger/adls/on-create-or-update");

        Map.Entry<ConditionContext, io.kestra.core.scheduler.model.TriggerState> context = TestsUtils.mockTrigger(runContextFactory, trigger);

        Optional<Execution> createExecution = trigger.evaluate(context.getKey(), context.getValue().context());
        assertThat(createExecution.isPresent(), is(true));

        update("trigger/adls/on-create-or-update");
        Thread.sleep(2000);

        Optional<Execution> updateExecution = trigger.evaluate(context.getKey(), context.getValue().context());
        assertThat(updateExecution.isPresent(), is(true));

        DeleteFiles cleaner = deleteDir("trigger/adls/on-create-or-update").build();
        cleaner.run(runContext(cleaner));
    }

}
