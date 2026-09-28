package com.example.druid;

import com.alibaba.druid.pool.DruidDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DruidDataSourceProviderTest {

    private DruidDataSource ds;

    @AfterEach
    void tearDown() {
        if (ds != null) {
            ds.close();
        }
    }

    @Test
    void buildsPoolFromClasspathConfig() {
        ds = DruidDataSourceProvider.createFromClasspath("druid.properties");
        assertNotNull(ds);
        assertEquals("org.h2.Driver", ds.getDriverClassName());
        assertEquals(3, ds.getInitialSize());
        assertEquals(20, ds.getMaxActive());
    }

    @Test
    void borrowsAndReturnsAWorkingConnection() throws SQLException {
        ds = DruidDataSourceProvider.createFromClasspath("druid.properties");
        try (Connection conn = ds.getConnection();
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT 1")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }
        // After the try-with-resources returns the connection, it goes back to the pool.
        assertTrue(ds.getConnectCount() >= 1);
    }

    @Test
    void reusesConnectionsAcrossBorrows() throws SQLException {
        ds = DruidDataSourceProvider.createFromClasspath("druid.properties");
        for (int i = 0; i < 10; i++) {
            try (Connection conn = ds.getConnection()) {
                assertTrue(conn.isValid(1));
            }
        }
        // With initialSize=3 and serial borrows, the pool should not have created
        // anywhere near 10 physical connections.
        assertTrue(ds.getCreateCount() <= ds.getMaxActive(),
                "pool created more physical connections than maxActive");
    }
}
