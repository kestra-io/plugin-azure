package io.kestra.plugin.azure.storage.adls;

import io.kestra.core.models.executions.Execution;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.FlowListeners;
import io.kestra.core.runners.Worker;
import io.kestra.core.schedulers.AbstractScheduler;
import io.kestra.core.utils.TestsUtils;
import io.kestra.jdbc.runner.JdbcScheduler;
import io.kestra.plugin.azure.storage.adls.models.AdlsFile;
import io.kestra.plugin.azure.storage.blob.models.Blob;
import io.micronaut.context.ApplicationContext;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@Disabled("Unit tests works correctly locally but fail on the CI - temporary disable them")
class TriggerTest extends AbstractTest {
    @Inject
    private ApplicationContext applicationContext;

    @Inject
    private FlowListeners flowListenersService;

    @Inject
    @Named(QueueFactoryInterface.EXECUTION_NAMED)
    private QueueInterface<Execution> executionQueue;

    @Inject
    protected LocalFlowRepositoryLoader repositoryLoader;

    @Test
    void trigger() throws Exception {
        // mock flow listeners
        CountDownLatch queueCount = new CountDownLatch(1);

        // scheduler
        Worker worker = applicationContext.createBean(Worker.class, UUID.randomUUID().toString(), 8, null);
        try (
            AbstractScheduler scheduler = new JdbcScheduler(
                this.applicationContext,
                this.flowListenersService
            );
        ) {
            AtomicReference<Execution> last = new AtomicReference<>();

            // wait for execution
            Flux<Execution> receive = TestsUtils.receive(executionQueue, executionWithError -> {
                Execution execution = executionWithError.getLeft();
                if (execution.getFlowId().equals("adls-listen")) {
                    last.set(execution);
                    queueCount.countDown();
                }
            });


            upload("adls/azure/trigger/adls-listen");
            upload("adls/azure/trigger/adls-listen");

            worker.run();
            scheduler.run();
            repositoryLoader.load(Objects.requireNonNull(TriggerTest.class.getClassLoader().getResource("flows/adls-listen.yaml")));

            boolean await = queueCount.await(10, TimeUnit.SECONDS);
            try {
                assertThat(await, is(true));
            } finally {
                worker.shutdown();
                receive.blockLast();
            }

            @SuppressWarnings("unchecked")
            java.util.List<AdlsFile> trigger = (java.util.List<AdlsFile>) last.get().getTrigger().getVariables().get("files");

            assertThat(trigger.size(), is(2));

            List listTask = list()
                .directoryPath("adls/azure/trigger/adls-listen")
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
        // mock flow listeners
        CountDownLatch queueCount = new CountDownLatch(1);

        // scheduler
        Worker worker = applicationContext.createBean(Worker.class, UUID.randomUUID().toString(), 8, null);
        try (
            AbstractScheduler scheduler = new JdbcScheduler(
                this.applicationContext,
                this.flowListenersService
            );
        ) {
            AtomicReference<Execution> last = new AtomicReference<>();

            // wait for execution
            Flux<Execution> receive = TestsUtils.receive(executionQueue, executionWithError -> {
                Execution execution = executionWithError.getLeft();
                if (execution.getFlowId().equals("adls-listen-delete-action")) {
                    last.set(execution);
                    queueCount.countDown();
                }
            });


            upload("adls/azure/trigger/adls-listen-delete-action");
            upload("adls/azure/trigger/adls-listen-delete-action");

            worker.run();
            scheduler.run();
            repositoryLoader.load(Objects.requireNonNull(TriggerTest.class.getClassLoader().getResource("flows/adls-listen-delete-action.yaml")));

            boolean await = queueCount.await(10, TimeUnit.SECONDS);
            try {
                assertThat(await, is(true));
            } finally {
                worker.shutdown();
                receive.blockLast();
            }

            @SuppressWarnings("unchecked")
            java.util.List<AdlsFile> trigger = (java.util.List<AdlsFile>) last.get().getTrigger().getVariables().get("files");

            assertThat(trigger.size(), is(2));

            List listTask = list()
                .directoryPath("adls/azure/trigger/adls-listen-delete-action")
                .build();

            int remainingFilesOnBucket = listTask.run(runContext(listTask))
                .getFiles()
                .size();
            assertThat(remainingFilesOnBucket, is(0));
        }
    }

    @Test
    void moveAction() throws Exception {
        // mock flow listeners
        CountDownLatch queueCount = new CountDownLatch(1);

        // scheduler
        Worker worker = applicationContext.createBean(Worker.class, UUID.randomUUID().toString(), 8, null);
        try (
            AbstractScheduler scheduler = new JdbcScheduler(
                this.applicationContext,
                this.flowListenersService
            );
        ) {
            AtomicReference<Execution> last = new AtomicReference<>();

            // wait for execution
            Flux<Execution> receive = TestsUtils.receive(executionQueue, executionWithError -> {
                Execution execution = executionWithError.getLeft();
                if (execution.getFlowId().equals("adls-listen-move-action")) {
                    last.set(execution);
                    queueCount.countDown();
                }
            });


            upload("adls/azure/trigger/adls-listen-move-action");
            upload("adls/azure/trigger/adls-listen-move-action");

            worker.run();
            scheduler.run();
            repositoryLoader.load(Objects.requireNonNull(TriggerTest.class.getClassLoader().getResource("flows/adls-listen-move-action.yaml")));

            boolean await = queueCount.await(10, TimeUnit.SECONDS);
            try {
                assertThat(await, is(true));
            } finally {
                worker.shutdown();
                receive.blockLast();
            }

            @SuppressWarnings("unchecked")
            java.util.List<AdlsFile> trigger = (java.util.List<AdlsFile>) last.get().getTrigger().getVariables().get("files");

            assertThat(trigger.size(), is(2));

            //Moved files
            List listTask = list()
                .directoryPath("adls/azure/trigger/adls-listen-move-action-direction")
                .build();

            int movedFilesOnBucket = listTask.run(runContext(listTask))
                .getFiles()
                .size();
            assertThat(movedFilesOnBucket, is(2));

            //Initial files
            List remainingFiles = list()
                .directoryPath("adls/azure/trigger/adls-listen-move-action")
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
}
