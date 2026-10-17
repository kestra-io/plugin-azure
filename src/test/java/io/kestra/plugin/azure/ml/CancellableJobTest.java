package io.kestra.plugin.azure.ml;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class CancellableJobTest {

    @Test
    void killBeforeArmRunsCancellationOnce() throws InterruptedException {
        CancellableJob job = new CancellableJob();
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch completed = new CountDownLatch(1);

        job.kill();
        job.arm(() -> {
            calls.incrementAndGet();
            completed.countDown();
        });

        assertTrue(completed.await(2, TimeUnit.SECONDS));
        assertEquals(1, calls.get());
    }

    @Test
    void killAfterArmRunsCancellationOnce() throws InterruptedException {
        CancellableJob job = new CancellableJob();
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch completed = new CountDownLatch(1);

        job.arm(() -> {
            calls.incrementAndGet();
            completed.countDown();
        });
        job.kill();

        assertTrue(completed.await(2, TimeUnit.SECONDS));
        assertEquals(1, calls.get());
    }

    @Test
    void repeatedKillDoesNotRepeatCancellation() throws InterruptedException {
        CancellableJob job = new CancellableJob();
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch completed = new CountDownLatch(1);

        job.arm(() -> {
            calls.incrementAndGet();
            started.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                completed.countDown();
            }
        });

        job.kill();
        assertTrue(started.await(2, TimeUnit.SECONDS));

        job.kill();
        job.kill();
        job.kill();

        release.countDown();
        assertTrue(completed.await(2, TimeUnit.SECONDS));
        assertEquals(1, calls.get());
    }
}
