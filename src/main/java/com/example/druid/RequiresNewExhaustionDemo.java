package com.example.druid;

import com.alibaba.druid.pool.DruidDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Simulates how Spring's {@code @Transactional(propagation = REQUIRES_NEW)}
 * can exhaust a JDBC connection pool and deadlock the application threads.
 *
 * <h2>The real-world scenario</h2>
 * <pre>
 *   &#64;Transactional                       // OUTER tx  -> borrows connection #1
 *   public void process() {
 *       ...
 *       auditService.record();           // calls the method below
 *   }
 *
 *   &#64;Transactional(propagation = REQUIRES_NEW)  // INNER tx -> borrows connection #2
 *   public void record() { ... }         //   WHILE #1 is still held!
 * </pre>
 *
 * <p>Each thread that enters {@code process()} holds its outer connection for
 * the entire duration of the outer transaction. When it then calls the
 * {@code REQUIRES_NEW} method, Spring suspends the outer transaction but does
 * <b>not</b> release its connection, and asks the pool for a brand new one.
 *
 * <p>So every in-flight request needs <b>two</b> connections at once. Once the
 * number of concurrent requests reaches {@code maxActive}, all connections are
 * held at the "outer" stage and every thread blocks forever waiting for an
 * "inner" connection that can only be freed by a thread that is itself
 * blocked. That is a classic self-inflicted deadlock; Druid breaks it by
 * timing out the borrow after {@code maxWait}.
 *
 * <p>This class reproduces the effect with plain JDBC (no Spring): a worker
 * borrows the "outer" connection, waits on a barrier so that all outer
 * connections are held at the same instant, then tries to borrow the "inner"
 * (REQUIRES_NEW) connection.
 */
public final class RequiresNewExhaustionDemo {

    /** More workers than the pool has connections -> guaranteed exhaustion. */
    private static final int CONCURRENT_REQUESTS = 8;

    public static void main(String[] args) throws Exception {
        DruidDataSource ds =
                DruidDataSourceProvider.createFromClasspath("druid-small-pool.properties");

        System.out.println("== REQUIRES_NEW pool-exhaustion simulation ==");
        System.out.printf("Pool maxActive       : %d%n", ds.getMaxActive());
        System.out.printf("maxWait (ms)         : %d%n", ds.getMaxWait());
        System.out.printf("Concurrent requests  : %d  (each needs 2 connections)%n",
                CONCURRENT_REQUESTS);
        System.out.printf("Connections demanded : %d  >> pool capacity %d%n%n",
                CONCURRENT_REQUESTS * 2, ds.getMaxActive());

        // Every worker parks here after grabbing its OUTER connection, so that
        // all outer connections are held simultaneously before any inner
        // borrow is attempted. This makes the exhaustion deterministic.
        CountDownLatch allOuterHeld = new CountDownLatch(CONCURRENT_REQUESTS);
        CountDownLatch releaseGate = new CountDownLatch(1);

        AtomicInteger succeeded = new AtomicInteger();
        AtomicInteger timedOut = new AtomicInteger();

        ExecutorService pool = Executors.newFixedThreadPool(CONCURRENT_REQUESTS);
        CountDownLatch done = new CountDownLatch(CONCURRENT_REQUESTS);

        for (int i = 0; i < CONCURRENT_REQUESTS; i++) {
            final int reqId = i;
            pool.submit(() -> {
                try {
                    runRequest(ds, reqId, allOuterHeld, releaseGate, succeeded, timedOut);
                } finally {
                    done.countDown();
                }
            });
        }

        // Wait until every worker is holding its outer connection (or the pool
        // already refused some outer borrows). Then open the gate so they all
        // race for an inner (REQUIRES_NEW) connection at once.
        allOuterHeld.await(10, TimeUnit.SECONDS);
        System.out.printf("[main] all outer connections held; pooling=%d active=%d%n",
                ds.getPoolingCount(), ds.getActiveCount());
        System.out.println("[main] opening gate -> every request now needs a 2nd connection\n");
        releaseGate.countDown();

        done.await();
        pool.shutdown();

        System.out.println();
        System.out.println("== Result ==");
        System.out.printf("Inner (REQUIRES_NEW) borrows that succeeded : %d%n", succeeded.get());
        System.out.printf("Inner (REQUIRES_NEW) borrows that timed out  : %d%n", timedOut.get());
        System.out.printf("Pool: active=%d pooling=%d connectErrorCount=%d%n",
                ds.getActiveCount(), ds.getPoolingCount(), ds.getConnectErrorCount());
        if (timedOut.get() > 0) {
            System.out.println();
            System.out.println(">> Pool exhausted: REQUIRES_NEW forced a 2nd concurrent borrow "
                    + "per request, exceeding maxActive. Blocked borrows failed after maxWait.");
        }

        DruidDataSourceProvider.closeSharedQuietly(ds);
    }

    private static void runRequest(DruidDataSource ds,
                                   int reqId,
                                   CountDownLatch allOuterHeld,
                                   CountDownLatch releaseGate,
                                   AtomicInteger succeeded,
                                   AtomicInteger timedOut) {
        Connection outer = null;
        try {
            // ---- OUTER @Transactional: borrow and hold connection #1 ----
            outer = ds.getConnection();
            outer.setAutoCommit(false);
            System.out.printf("[req-%d] OUTER connection acquired%n", reqId);

            // Signal that this request is holding its outer connection, then
            // wait for the gate so all requests attempt the inner borrow together.
            allOuterHeld.countDown();
            releaseGate.await();

            // ---- INNER @Transactional(REQUIRES_NEW): borrow connection #2 ----
            // The outer connection is STILL held here (tx suspended, not closed).
            System.out.printf("[req-%d] entering REQUIRES_NEW, asking pool for a 2nd connection...%n",
                    reqId);
            long start = System.currentTimeMillis();
            try (Connection inner = ds.getConnection()) {
                inner.setAutoCommit(false);
                long waited = System.currentTimeMillis() - start;
                System.out.printf("[req-%d] INNER connection acquired after %dms%n", reqId, waited);
                inner.commit();
                succeeded.incrementAndGet();
            }

            outer.commit();
        } catch (SQLException e) {
            long timedOutCount = timedOut.incrementAndGet();
            System.out.printf("[req-%d] INNER borrow FAILED (#%d): %s%n",
                    reqId, timedOutCount, rootMessage(e));
            rollbackQuietly(outer);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            closeQuietly(outer);
        }
    }

    private static String rootMessage(Throwable t) {
        String simple = t.getClass().getSimpleName();
        String msg = t.getMessage();
        if (msg != null && msg.length() > 120) {
            msg = msg.substring(0, 120) + "...";
        }
        return simple + ": " + msg;
    }

    private static void rollbackQuietly(Connection c) {
        if (c != null) {
            try {
                c.rollback();
            } catch (SQLException ignored) {
                // best effort
            }
        }
    }

    private static void closeQuietly(Connection c) {
        if (c != null) {
            try {
                c.close();
            } catch (SQLException ignored) {
                // best effort
            }
        }
    }

    private RequiresNewExhaustionDemo() {
    }
}
