# druid-connection-exp

Experiments with the [Alibaba Druid](https://github.com/alibaba/druid) JDBC connection pool.

The project builds a `DruidDataSource` from a properties file, runs concurrent
queries against an embedded H2 database, and prints the pool's live statistics.
It uses in-memory H2 so it runs with **no external database setup**.

## Requirements

- JDK 17+
- Maven 3.8+

## Layout

```
src/main/java/com/example/druid/
  DruidConnectionDemo.java              # runnable demo: schema, seed, concurrent queries, stats
  RequiresNewExhaustionDemo.java        # plain-JDBC simulation of REQUIRES_NEW pool exhaustion
  DruidDataSourceProvider.java          # builds/holds the DruidDataSource from config
  spring/
    AppConfig.java                      # Spring config: DataSource + tx manager + @EnableTransactionManagement
    OrderService.java                   # outer @Transactional(REQUIRED) method
    AuditService.java                   # inner @Transactional(REQUIRES_NEW) method
    SpringRequiresNewExhaustionDemo.java# Spring-annotation-driven exhaustion runner
src/main/resources/
  druid.properties                      # main connection pool configuration
  druid-small-pool.properties           # tiny pool (maxActive=5) used by the exhaustion demos
  simplelogger.properties               # slf4j-simple logging config
src/test/java/com/example/druid/
  DruidDataSourceProviderTest.java      # JUnit 5 tests for pool creation & connection reuse
```

## Build and test

```bash
mvn clean test        # run the unit tests
mvn clean package     # build a runnable fat jar in target/
```

## Run the demo

```bash
java -jar target/druid-connection-exp.jar
```

Expected output (abbreviated):

```
== Druid connection pool experiment ==
Driver           : org.h2.Driver
JDBC URL         : jdbc:h2:mem:druidexp;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE
Initial size     : 3
Max active       : 20

[schema] table 'widget' created
[seed] inserted 25 rows
[worker-0] counted 25 widgets
...
== Pool statistics ==
Active connections : 0
Idle (pooled)      : 3
Connect count      : 10
...
Pool closed. Done.
```

## Configuration

Pool behavior is driven entirely by [`druid.properties`](src/main/resources/druid.properties).
Key settings:

| Property | Meaning |
| --- | --- |
| `druid.initialSize` | connections created at startup |
| `druid.minIdle` / `druid.maxActive` | min idle / max concurrent connections |
| `druid.maxWait` | max ms to wait for a connection before failing |
| `druid.validationQuery` | query used to validate connections |
| `druid.testWhileIdle` | validate idle connections during eviction runs |
| `druid.poolPreparedStatements` | enable server-side prepared statement caching |
| `druid.filters` | Druid filters to load (`stat` = SQL statistics) |

### Pointing at a real database

Swap the JDBC settings in `druid.properties`, for example MySQL:

```properties
druid.url=jdbc:mysql://localhost:3306/mydb?useSSL=false&serverTimezone=UTC
druid.username=app
druid.password=secret
druid.driverClassName=com.mysql.cj.jdbc.Driver
```

Then add the matching JDBC driver dependency to `pom.xml`.

## Experiment: `REQUIRES_NEW` connection-pool exhaustion

A common production incident: a method running in an outer transaction calls
another method annotated `@Transactional(propagation = REQUIRES_NEW)`. Spring
**suspends** the outer transaction but keeps its connection bound to the thread,
then borrows a **second** connection for the inner transaction. So every
in-flight request holds two connections at once.

Once the number of concurrent requests reaches `maxActive`, all connections are
held at the outer stage and every thread blocks waiting for an inner connection
that can only be freed by another blocked thread — a self-inflicted deadlock.
Druid breaks the deadlock by aborting each blocked borrow after `maxWait`.

Both demos use [`druid-small-pool.properties`](src/main/resources/druid-small-pool.properties)
(`maxActive=5`, `maxWait=3000ms`) and fire 8 concurrent requests (16 connections
demanded against a pool of 5).

### Spring-annotation version (faithful reproduction)

Uses a real `DataSourceTransactionManager`. `OrderService.placeOrder` is
`@Transactional`; it calls `AuditService.record` which is
`@Transactional(REQUIRES_NEW)`. The inner call crosses a Spring proxy boundary
(a separate bean) so the new-transaction semantics actually apply.

```bash
mvn compile
CP="target/classes:$(mvn -q dependency:build-classpath -Dmdep.outputFile=/dev/stdout | tail -1)"
java -cp "$CP" com.example.druid.spring.SpringRequiresNewExhaustionDemo
```

Output (abbreviated):

```
== Spring @Transactional(REQUIRES_NEW) pool-exhaustion demo ==
Pool maxActive       : 5
Concurrent requests  : 8  (each needs 2 connections via REQUIRES_NEW)
...
[order-1] FAILED: CannotCreateTransactionException -> GetConnectionTimeoutException: wait millis 3005, active 5, maxActive 5
...
Requests completed : 0
Requests failed    : 8
```

Spring surfaces the pool timeout as `CannotCreateTransactionException` at the
moment the transaction manager tries to open the `REQUIRES_NEW` transaction.

### Plain-JDBC version

The same effect without Spring: each worker borrows an "outer" connection, waits
on a barrier, then tries to borrow an "inner" connection.

```bash
java -cp "$CP" com.example.druid.RequiresNewExhaustionDemo
```

### How to avoid it in real code

- Don't call `REQUIRES_NEW` methods from inside a long-running outer transaction
  on the same data source; do the independent work before/after the outer tx.
- Ensure `maxActive` comfortably exceeds `peak_concurrent_requests × max_nested_tx_depth`.
- Keep transactions short so connections return to the pool quickly.
- Always set a finite `maxWait` so a starved pool fails fast instead of hanging.

## License

Released under the [MIT License](LICENSE).
