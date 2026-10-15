package io.kestra.plugin.azure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

class CancellableOperationTest {

    @Test
    void killBeforeArm_cancelsWhenActionBecomesAvailable() throws InterruptedException {
        CancellableOperation lifecycle = new CancellableOperation("operation");
        CountDownLatch cancelled = new CountDownLatch(1);

        lifecycle.kill();
        lifecycle.arm(cancelled::countDown);

        assertTrue(cancelled.await(5, TimeUnit.SECONDS));
    }

    @Test
    void killAfterArm_invokesCancellation() throws InterruptedException {
        CancellableOperation lifecycle = new CancellableOperation("operation");
        CountDownLatch cancelled = new CountDownLatch(1);

        lifecycle.arm(cancelled::countDown);
        lifecycle.kill();

        assertTrue(cancelled.await(5, TimeUnit.SECONDS));
    }

    @Test
    void repeatedKill_invokesCancellationOnlyOnce() throws InterruptedException {
        CancellableOperation lifecycle = new CancellableOperation("operation");
        CountDownLatch cancelled = new CountDownLatch(1);
        AtomicInteger invocationCount = new AtomicInteger();

        lifecycle.arm(() -> {
            invocationCount.incrementAndGet();
            cancelled.countDown();
        });

        lifecycle.kill();
        assertTrue(cancelled.await(5, TimeUnit.SECONDS));

        lifecycle.kill();
        lifecycle.kill();
        Thread.sleep(200);

        assertEquals(1, invocationCount.get());
    }

    @Test
    void disarmBeforeKill_doesNotInvokeCancellation() throws InterruptedException {
        CancellableOperation lifecycle = new CancellableOperation("operation");
        CountDownLatch cancelled = new CountDownLatch(1);

        lifecycle.arm(cancelled::countDown);
        lifecycle.disarm();
        lifecycle.kill();

        assertFalse(cancelled.await(200, TimeUnit.MILLISECONDS));
    }

    @Test
    void concurrentArmAndKill_doesNotLoseCancellation() throws InterruptedException {
        CancellableOperation lifecycle = new CancellableOperation("operation");
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch cancelled = new CountDownLatch(1);

        Thread armThread = new Thread(() -> {
            try {
                start.await();
                lifecycle.arm(cancelled::countDown);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        Thread killThread = new Thread(() -> {
            try {
                start.await();
                lifecycle.kill();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        armThread.start();
        killThread.start();
        start.countDown();
        armThread.join(2_000);
        killThread.join(2_000);

        assertFalse(armThread.isAlive(), "arm thread should finish");
        assertFalse(killThread.isAlive(), "kill thread should finish");
        assertTrue(cancelled.await(5, TimeUnit.SECONDS));
    }
}
