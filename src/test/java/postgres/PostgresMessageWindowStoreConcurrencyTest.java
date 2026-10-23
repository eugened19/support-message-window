
package postgres;

import domain.MessageLimitExceededException;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import service.MessageWindowService;
import service.MessageWindowServiceImpl;
import store.postgres.PostgresMessageWindowStore;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class PostgresMessageWindowStoreConcurrencyTest {

    private static final int MESSAGE_LIMIT = 10;
    private static final Instant NOW =
        Instant.parse("2026-10-23T12:00:00Z");

    private DataSource createDataSource() {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();

        dataSource.setURL(
            System.getenv().getOrDefault(
                "TEST_DB_URL",
                "jdbc:postgresql://localhost:5432/support_message_window_db"
            )
        );
        dataSource.setUser(
            System.getenv().getOrDefault(
                "TEST_DB_USER",
                "support_message_window"
            )
        );
        dataSource.setPassword(
            System.getenv("TEST_DB_PASSWORD")
        );

        return dataSource;
    }

    @Test
    void onlyOneRequestGetsTheLastAvailableSlot() throws Exception {

        DataSource dataSource = createDataSource();

        // A unique agent isolates this test from previous test runs.
        // No DELETE or TRUNCATE privileges are required.
        String agent = "test-" + UUID.randomUUID();

        long agentId;

        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement("""
                 INSERT INTO agents (agent_name)
                 VALUES (?)
                 RETURNING agent_id
                 """)) {

            statement.setString(1, agent);

            try (ResultSet result = statement.executeQuery()) {
                assertTrue(result.next());
                agentId = result.getLong(1);
            }
        }

        // Prepare 9 messages within the active 30-minute window.
        // Exactly one slot remains.
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement("""
                 INSERT INTO agent_messages (agent_id, received_at)
                 VALUES (?, ?)
                 """)) {

            for (int i = 0; i < MESSAGE_LIMIT - 1; i++) {
                statement.setLong(1, agentId);
                statement.setObject(
                    2,
                    java.time.OffsetDateTime.ofInstant(
                        NOW.minusSeconds(i),
                        ZoneOffset.UTC
                    )
                );
                statement.addBatch();
            }

            statement.executeBatch();
        }

        MessageWindowService service = new MessageWindowServiceImpl(
            new PostgresMessageWindowStore(dataSource),
            MESSAGE_LIMIT,
            Clock.fixed(NOW, ZoneOffset.UTC)
        );

        CountDownLatch readyGate = new CountDownLatch(2);
        CountDownLatch startGate = new CountDownLatch(1);

        ExecutorService executor = Executors.newFixedThreadPool(2);

        try {
            // Each task obtains its own JDBC connection through the store.
            // The shared latch makes both tasks start together.
            java.util.concurrent.Callable<Boolean> attempt = () -> {
                readyGate.countDown();
                startGate.await();

                try {
                    service.addMessage(agent, "Hello");
                    return true;
                } catch (MessageLimitExceededException e) {
                    return false;
                }
            };

            Future<Boolean> first = executor.submit(attempt);
            Future<Boolean> second = executor.submit(attempt);

            assertTrue(
                readyGate.await(5, TimeUnit.SECONDS),
                "Both worker threads should be ready"
            );

            startGate.countDown();

            boolean firstAccepted = first.get(10, TimeUnit.SECONDS);
            boolean secondAccepted = second.get(10, TimeUnit.SECONDS);

            // Exactly one request must succeed.
            assertEquals(
                1,
                (firstAccepted ? 1 : 0) + (secondAccepted ? 1 : 0)
            );

            // Verify the database itself, not just returned values.
            try (Connection connection = dataSource.getConnection();
                 PreparedStatement statement = connection.prepareStatement("""
                     SELECT COUNT(*)
                     FROM agent_messages
                     WHERE agent_id = ?
                     """)) {

                statement.setLong(1, agentId);

                try (ResultSet result = statement.executeQuery()) {
                    assertTrue(result.next());
                    assertEquals(MESSAGE_LIMIT, result.getInt(1));
                }
            }

        } finally {
            startGate.countDown();
            executor.shutdownNow();
        }
    }
}
