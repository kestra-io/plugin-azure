package io.kestra.plugin.azure.storage.blob;

import io.kestra.core.models.executions.Execution;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.FlowListeners;
import io.kestra.core.runners.Worker;
import io.kestra.core.schedulers.AbstractScheduler;
import io.kestra.core.schedulers.DefaultScheduler;
import io.kestra.core.schedulers.SchedulerTriggerStateInterface;
import io.kestra.plugin.azure.storage.blob.models.Blob;
import io.micronaut.context.ApplicationContext;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.Test;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class TriggerTest extends AbstractTest {
    @Inject
    private ApplicationContext applicationContext;

    @Inject
    private SchedulerTriggerStateInterface triggerState;

    @Inject
    private FlowListeners flowListenersService;

    @Inject
    @Named(QueueFactoryInterface.EXECUTION_NAMED)
    private QueueInterface<Execution> executionQueue;

    @Inject
    protected LocalFlowRepositoryLoader repositoryLoader;

    @Test
    void deleteAction() throws Exception {
        // mock flow listeners
        CountDownLatch queueCount = new CountDownLatch(1);

        // scheduler
        Worker worker = applicationContext.createBean(Worker.class, UUID.randomUUID().toString(), 8, null);
        try (
            AbstractScheduler scheduler = new DefaultScheduler(
                this.applicationContext,
                this.flowListenersService,
                this.triggerState
            );
        ) {
            AtomicReference<Execution> last = new AtomicReference<>();

            // wait for execution
            executionQueue.receive(TriggerTest.class, executionWithError -> {
                Execution execution = executionWithError.getLeft();
                if (execution.getFlowId().equals("blob-storage-listen")) {
                    last.set(execution);
                    queueCount.countDown();
                }
            });


            String toUploadDir = "trigger/storage-listen";
            upload(toUploadDir);
            upload(toUploadDir);

            worker.run();
            scheduler.run();
            repositoryLoader.load(Objects.requireNonNull(TriggerTest.class.getClassLoader().getResource("flows/blob-storage-listen.yaml")));

            boolean await = queueCount.await(10, TimeUnit.SECONDS);
            try {
                assertThat(await, is(true));
            } finally {
                worker.shutdown();
            }

            @SuppressWarnings("unchecked")
            java.util.List<Blob> trigger = (java.util.List<Blob>) last.get().getTrigger().getVariables().get("blobs");

            assertThat(trigger.size(), is(2));

            List listTask = list()
                .prefix(toUploadDir)
                .build();
            int remainingFilesOnBucket = listTask.run(runContext(listTask))
                .getBlobs()
                .size();
            assertThat(remainingFilesOnBucket, is(0));
        }
    }

    @Test
    void noneAction() throws Exception {
        // mock flow listeners
        CountDownLatch queueCount = new CountDownLatch(1);

        // scheduler
        Worker worker = applicationContext.createBean(Worker.class, UUID.randomUUID().toString(), 8, null);
        try (
            AbstractScheduler scheduler = new DefaultScheduler(
                this.applicationContext,
                this.flowListenersService,
                this.triggerState
            );
        ) {
            AtomicReference<Execution> last = new AtomicReference<>();

            // wait for execution
            executionQueue.receive(TriggerTest.class, executionWithError -> {
                Execution execution = executionWithError.getLeft();
                if (execution.getFlowId().equals("blob-storage-listen-none-action")) {
                    last.set(execution);
                    queueCount.countDown();
                }
            });


            upload("trigger/none-action-storage-listen");
            upload("trigger/none-action-storage-listen");

            worker.run();
            scheduler.run();
            repositoryLoader.load(Objects.requireNonNull(TriggerTest.class.getClassLoader().getResource("flows/blob-storage-listen-none-action.yaml")));

            boolean await = queueCount.await(10, TimeUnit.SECONDS);
            try {
                assertThat(await, is(true));
            } finally {
                worker.shutdown();
            }

            @SuppressWarnings("unchecked")
            java.util.List<Blob> trigger = (java.util.List<Blob>) last.get().getTrigger().getVariables().get("blobs");

            assertThat(trigger.size(), is(2));

            List listTask = list()
                .prefix("trigger/none-action-storage-listen")
                .build();
            int remainingFilesOnBucket = listTask.run(runContext(listTask))
                .getBlobs()
                .size();
            assertThat(remainingFilesOnBucket, is(2));
        } finally {
            DeleteList cleaner = deleteDir("trigger/none-action-storage-listen").build();
            cleaner.run(runContext(cleaner));
        }
    }
}
