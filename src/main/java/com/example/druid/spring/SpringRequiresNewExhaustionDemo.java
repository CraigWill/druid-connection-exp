package com.example.druid.spring;

import com.alibaba.druid.pool.DruidDataSource;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Spring-annotation-driven reproduction of {@code REQUIRES_NEW} connection-pool
 * exhaustion.
 *
 * <p>Unlike the plain-JDBC {@code RequiresNewExhaustionDemo}, this version uses
 * the real Spring transaction manager:
 * <ul>
 *   <li>{@link OrderService#placeOrder} is {@code @Transactional} (REQUIRED) —
 *       it borrows and holds connection #1,</li>
 *   <li>it calls {@link AuditService#record} which is
 *       {@code @Transactional(REQUIRES_NEW)} — Spring suspends the outer tx and
 *       borrows connection #2 while #1 is still held.</li>
 * </ul>
 *
 * <p>With {@code maxActive=5} and 8 concurrent requests, all five connections
 * end up held at the outer stage and the inner {@code REQUIRES_NEW} borrows
 * starve. Druid aborts each blocked borrow after {@code maxWait}, surfaced by
 * Spring as {@code CannotCreateTransactionException} /
 * {@code CannotGetJdbcConnectionException}.
 */
public final class SpringRequiresNewExhaustionDemo {

    private static final int CONCURRENT_REQUESTS = 8;

    public static void main(String[] args) throws Exception {
        AnnotationConfigApplicationContext ctx =
                new AnnotationConfigApplicationContext(AppConfig.class);

        DruidDataSource ds = ctx.getBean(DruidDataSource.class);
        OrderService orderService = ctx.getBean(OrderService.class);

        createSchema(new JdbcTemplate(ds));

        System.out.println("== Spring @Transactional(REQUIRES_NEW) pool-exhaustion demo ==");
        System.out.printf("Pool maxActive       : %d%n", ds.getMaxActive());
        System.out.printf("maxWait (ms)         : %d%n", ds.getMaxWait());
        System.out.printf("Concurrent requests  : %d  (each needs 2 connections via REQUIRES_NEW)%n",
                CONCURRENT_REQUESTS);
        System.out.printf("Connections demanded : %d  >> pool capacity %d%n%n",
                CONCURRENT_REQUESTS * 2, ds.getMaxActive());

        CountDownLatch outerHeld = new CountDownLatch(CONCURRENT_REQUESTS);
        CountDownLatch releaseGate = new CountDownLatch(1);
        orderService.configureBarrier(outerHeld, releaseGate);

        AtomicInteger succeeded = new AtomicInteger();
        AtomicInteger failed = new AtomicInteger();

        ExecutorService pool = Executors.newFixedThreadPool(CONCURRENT_REQUESTS);
        CountDownLatch done = new CountDownLatch(CONCURRENT_REQUESTS);

        for (int i = 0; i < CONCURRENT_REQUESTS; i++) {
            final int orderId = i;
            pool.submit(() -> {
                try {
                    orderService.placeOrder(orderId);
                    System.out.printf("[order-%d] COMPLETED (outer + REQUIRES_NEW audit committed)%n",
                            orderId);
                    succeeded.incrementAndGet();
                } catch (Exception e) {
                    failed.incrementAndGet();
                    System.out.printf("[order-%d] FAILED: %s%n", orderId, rootMessage(e));
                } finally {
                    done.countDown();
                }
            });
        }

        // Wait until every request holds its outer connection, then release them
        // all to fight over the (already exhausted) pool for the inner borrow.
        outerHeld.await(10, TimeUnit.SECONDS);
        System.out.printf("[main] outer connections held: active=%d pooling=%d%n",
                ds.getActiveCount(), ds.getPoolingCount());
        System.out.println("[main] opening gate -> REQUIRES_NEW now demands a 2nd connection each\n");
        releaseGate.countDown();

        done.await();
        pool.shutdown();

        System.out.println();
        System.out.println("== Result ==");
        System.out.printf("Requests completed : %d%n", succeeded.get());
        System.out.printf("Requests failed    : %d%n", failed.get());
        System.out.printf("Pool: active=%d pooling=%d connectErrorCount=%d%n",
                ds.getActiveCount(), ds.getPoolingCount(), ds.getConnectErrorCount());
        if (failed.get() > 0) {
            System.out.println();
            System.out.println(">> REQUIRES_NEW made each request hold 2 connections at once. "
                    + "Once outer borrows filled the pool (maxActive=" + ds.getMaxActive()
                    + "), the inner borrows timed out after maxWait.");
        }

        ctx.close();
        System.out.println("\nSpring context closed. Done.");
    }

    private static void createSchema(JdbcTemplate jdbc) {
        jdbc.execute("DROP TABLE IF EXISTS orders");
        jdbc.execute("DROP TABLE IF EXISTS audit_log");
        jdbc.execute("CREATE TABLE orders (id INT PRIMARY KEY, description VARCHAR(64))");
        jdbc.execute("CREATE TABLE audit_log ("
                + "seq INT AUTO_INCREMENT PRIMARY KEY, "
                + "order_id INT NOT NULL, "
                + "note VARCHAR(128))");
    }

    private static String rootMessage(Throwable t) {
        Throwable root = t;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String msg = root.getMessage();
        if (msg != null && msg.length() > 140) {
            msg = msg.substring(0, 140) + "...";
        }
        return t.getClass().getSimpleName() + " -> " + root.getClass().getSimpleName() + ": " + msg;
    }

    private SpringRequiresNewExhaustionDemo() {
    }
}
