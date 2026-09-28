package com.example.druid;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Builds and holds a single {@link DruidDataSource} configured from
 * {@code druid.properties} on the classpath.
 *
 * <p>The pool is expensive to create, so a process normally keeps exactly one
 * instance and shares it. This provider exposes a lazily-initialised singleton
 * plus a helper to build ad-hoc data sources for experiments.
 */
public final class DruidDataSourceProvider {

    private static final String CONFIG_FILE = "druid.properties";

    private static volatile DruidDataSource sharedInstance;

    private DruidDataSourceProvider() {
    }

    /**
     * Returns the process-wide shared data source, creating it on first use.
     */
    public static DruidDataSource shared() {
        DruidDataSource local = sharedInstance;
        if (local == null) {
            synchronized (DruidDataSourceProvider.class) {
                local = sharedInstance;
                if (local == null) {
                    local = createFromClasspath(CONFIG_FILE);
                    sharedInstance = local;
                }
            }
        }
        return local;
    }

    /**
     * Creates a brand new {@link DruidDataSource} from a properties file found
     * on the classpath. Callers own the returned instance and must close it.
     *
     * @param resourceName classpath resource, e.g. {@code "druid.properties"}
     * @return a configured, ready-to-use data source
     */
    public static DruidDataSource createFromClasspath(String resourceName) {
        Properties props = loadProperties(resourceName);
        try {
            return (DruidDataSource) DruidDataSourceFactory.createDataSource(props);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to create DruidDataSource from " + resourceName, e);
        }
    }

    private static Properties loadProperties(String resourceName) {
        Properties props = new Properties();
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        try (InputStream in = cl.getResourceAsStream(resourceName)) {
            if (in == null) {
                throw new IllegalStateException(
                        "Configuration resource not found on classpath: " + resourceName);
            }
            props.load(in);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read " + resourceName, e);
        }
        return props;
    }

    /**
     * Closes the shared data source if it was created. Safe to call multiple times.
     */
    public static synchronized void closeShared() {
        if (sharedInstance != null) {
            sharedInstance.close();
            sharedInstance = null;
        }
    }

    /** Convenience accessor typed as the JDBC {@link DataSource} interface. */
    public static DataSource asDataSource() {
        return shared();
    }

    /**
     * Closes an ad-hoc data source (one built via {@link #createFromClasspath})
     * without throwing. Safe to pass {@code null}.
     */
    public static void closeSharedQuietly(DruidDataSource ds) {
        if (ds != null) {
            ds.close();
        }
    }
}
