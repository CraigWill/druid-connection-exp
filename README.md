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
  DruidConnectionDemo.java        # runnable demo: schema, seed, concurrent queries, stats
  DruidDataSourceProvider.java    # builds/holds the DruidDataSource from config
src/main/resources/
  druid.properties                # connection pool configuration
  simplelogger.properties         # slf4j-simple logging config
src/test/java/com/example/druid/
  DruidDataSourceProviderTest.java# JUnit 5 tests for pool creation & connection reuse
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

## License

Released under the [MIT License](LICENSE).
