package com.example.druid.spring;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;

/**
 * The "inner" service. Its {@link #record} method runs in its OWN transaction
 * via {@link Propagation#REQUIRES_NEW}, so when {@code OrderService} calls it,
 * Spring <b>suspends</b> the caller's transaction (keeping that connection
 * bound to the thread but idle) and borrows a <b>second</b> connection from the
 * pool for this method.
 *
 * <p>That second concurrent borrow, multiplied across many in-flight requests,
 * is what drives the pool past {@code maxActive}.
 */
public class AuditService {

    private final JdbcTemplate jdbcTemplate;

    public AuditService(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void record(int orderId) {
        // Needs a brand-new connection while the caller's connection is still held.
        jdbcTemplate.update(
                "INSERT INTO audit_log (order_id, note) VALUES (?, ?)",
                orderId, "order " + orderId + " audited");
    }
}
