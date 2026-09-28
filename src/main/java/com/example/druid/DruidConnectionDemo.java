package com.example.druid;

import com.alibaba.druid.pool.DruidDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * End-to-end demonstration of the Alibaba Druid connection pool.
 *
 * <p>Run with: {@code java -jar target/druid-connection-exp.jar}
 *
 * <p>The demo:
 * <ol>
 *   <li>builds the pool from {@code druid.properties},</li>
 *   <li>creates a schema and inserts rows,</li>
 *   <li>runs concurrent queries that borrow/return connections,</li>
 *   <li>prints the pool's live statistics.</li>
 * </ol>
 */
public final class DruidConnectionDemo {

    public static void main(String[] args) throws Exception {
        DruidDataSource ds = DruidDataSourceProvider.shared();
        try {
            System.out.println("== Druid connection pool experiment ==");
            System.out.printf("Driver           : %s%n", ds.getDriverClassName());
            System.out.printf("JDBC URL         : %s%n", ds.getUrl());
            System.out.printf("Initial size     : %d%n", ds.getInitialSize());
            System.out.printf("Max active       : %d%n", ds.getMaxActive());
            System.out.println();

            initSchema(ds);
            seedData(ds);
            runConcurrentQueries(ds, 8);
            printPoolStats(ds);
        } finally {
            DruidDataSourceProvider.closeShared();
            System.out.println();
            System.out.println("Pool closed. Done.");
        }
    }

    private static void initSchema(DruidDataSource ds) throws SQLException {
        try (Connection conn = ds.getConnection();
             Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS widget");
            st.execute("""
                    CREATE TABLE widget (
                        id     INT PRIMARY KEY,
                        name   VARCHAR(64) NOT NULL,
                        price  DECIMAL(10, 2) NOT NULL
                    )
                    """);
        }
        System.out.println("[schema] table 'widget' created");
    }

    private static void seedData(DruidDataSource ds) throws SQLException {
        String sql = "INSERT INTO widget (id, name, price) VALUES (?, ?, ?)";
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 1; i <= 25; i++) {
                ps.setInt(1, i);
                ps.setString(2, "widget-" + i);
                ps.setBigDecimal(3, java.math.BigDecimal.valueOf(i * 1.5));
                ps.addBatch();
            }
            int[] counts = ps.executeBatch();
            System.out.printf("[seed] inserted %d rows%n", counts.length);
        }
    }

    private static void runConcurrentQueries(DruidDataSource ds, int threads)
            throws InterruptedException {
        Thread[] workers = new Thread[threads];
        for (int t = 0; t < threads; t++) {
            final int workerId = t;
            workers[t] = new Thread(() -> {
                try {
                    long total = countWidgets(ds);
                    System.out.printf("[worker-%d] counted %d widgets%n", workerId, total);
                } catch (SQLException e) {
                    System.err.printf("[worker-%d] failed: %s%n", workerId, e.getMessage());
                }
            }, "druid-worker-" + t);
        }
        for (Thread w : workers) {
            w.start();
        }
        for (Thread w : workers) {
            w.join();
        }
    }

    private static long countWidgets(DruidDataSource ds) throws SQLException {
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM widget");
             ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getLong(1) : 0L;
        }
    }

    private static void printPoolStats(DruidDataSource ds) {
        System.out.println();
        System.out.println("== Pool statistics ==");
        System.out.printf("Active connections : %d%n", ds.getActiveCount());
        System.out.printf("Idle (pooled)      : %d%n", ds.getPoolingCount());
        System.out.printf("Connect count      : %d%n", ds.getConnectCount());
        System.out.printf("Close count        : %d%n", ds.getCloseCount());
        System.out.printf("Peak active        : %d%n", ds.getActivePeak());
    }

    private DruidConnectionDemo() {
    }
}
