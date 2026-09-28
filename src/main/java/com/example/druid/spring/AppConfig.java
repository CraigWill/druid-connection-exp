package com.example.druid.spring;

import com.alibaba.druid.pool.DruidDataSource;
import com.example.druid.DruidDataSourceProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.sql.DataSource;

/**
 * Spring configuration wiring the Druid small pool through Spring's
 * transaction infrastructure.
 *
 * <p>{@link EnableTransactionManagement} turns on the AOP proxying that makes
 * {@code @Transactional} (including {@code REQUIRES_NEW}) actually do something.
 * The {@link DataSourceTransactionManager} is what binds a JDBC connection to
 * the current thread for the life of a transaction and suspends/resumes it for
 * {@code REQUIRES_NEW} — which is exactly what causes a single request to hold
 * two connections at once.
 */
@Configuration
@EnableTransactionManagement
public class AppConfig {

    /**
     * The Druid pool from {@code druid-small-pool.properties}
     * (maxActive=5, maxWait=3000ms). Spring will close it on context shutdown.
     */
    @Bean(destroyMethod = "close")
    public DruidDataSource dataSource() {
        return DruidDataSourceProvider.createFromClasspath("druid-small-pool.properties");
    }

    @Bean
    public PlatformTransactionManager transactionManager(DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }

    @Bean
    public AuditService auditService(DataSource dataSource) {
        return new AuditService(dataSource);
    }

    @Bean
    public OrderService orderService(AuditService auditService, DataSource dataSource) {
        return new OrderService(auditService, dataSource);
    }
}
