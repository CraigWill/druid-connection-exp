package com.example.druid.spring;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * The "outer" service. {@link #placeOrder} runs in a normal (REQUIRED)
 * transaction, so entering it borrows connection #1 and keeps it bound to the
 * thread for the whole method.
 *
 * <p>Midway through, it calls {@link AuditService#record} which is annotated
 * {@code REQUIRES_NEW}. Because {@code auditService} is a <b>separate Spring
 * bean</b>, the call goes through Spring's proxy and the new-transaction
 * semantics actually apply — Spring suspends this method's transaction (without
 * releasing connection #1) and borrows connection #2 for the audit.
 *
 * <p>A {@link CountDownLatch} barrier makes every concurrent request hold its
 * outer connection at the same instant before any inner borrow happens, so the
 * exhaustion is deterministic rather than timing-dependent.
 */
public class OrderService {

    private final AuditService auditService;
    private final JdbcTemplate jdbcTemplate;

    /** Signalled once this request holds its outer connection. */
    private volatile CountDownLatch outerHeldSignal;
    /** Requests wait on this before attempting the inner (REQUIRES_NEW) borrow. */
    private volatile CountDownLatch releaseGate;

    public OrderService(AuditService auditService, DataSource dataSource) {
        this.auditService = auditService;
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    public void configureBarrier(CountDownLatch outerHeldSignal, CountDownLatch releaseGate) {
        this.outerHeldSignal = outerHeldSignal;
        this.releaseGate = releaseGate;
    }

    @Transactional(propagation = Propagation.REQUIRED)
    public void placeOrder(int orderId) {
        // OUTER transaction is now active -> connection #1 is bound to this thread.
        jdbcTemplate.update(
                "INSERT INTO orders (id, description) VALUES (?, ?)",
                orderId, "order-" + orderId);

        // Announce that this request is holding its outer connection, then wait
        // so that ALL requests hold their outer connection before any inner borrow.
        if (outerHeldSignal != null) {
            outerHeldSignal.countDown();
        }
        awaitGate();

        // INNER call across the proxy: REQUIRES_NEW -> suspends this tx (keeps
        // connection #1) and borrows connection #2. This is where the pool runs dry.
        auditService.record(orderId);
    }

    private void awaitGate() {
        CountDownLatch gate = this.releaseGate;
        if (gate != null) {
            try {
                gate.await(15, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
