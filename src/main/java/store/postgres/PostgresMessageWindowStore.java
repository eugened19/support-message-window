
package store.postgres;

import store.MessageWindowStore;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;

public class PostgresMessageWindowStore implements MessageWindowStore {

    private static final Duration WINDOW = Duration.ofMinutes(30);

    private final DataSource dataSource;

    public PostgresMessageWindowStore(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public int countActiveMessages(String agent, Instant now) {

        String sql = """
            SELECT COUNT(*)
            FROM agent_messages m
            JOIN agents a ON a.agent_id = m.agent_id
            WHERE a.agent_name = ?
              AND m.received_at >= ?
              AND m.received_at <= ?
            """;

        Instant cutoff = now.minus(WINDOW);

        try (
            Connection connection = dataSource.getConnection();
            PreparedStatement statement = connection.prepareStatement(sql)
        ) {
            statement.setString(1, agent);
            statement.setTimestamp(2, Timestamp.from(cutoff));
            statement.setTimestamp(3, Timestamp.from(now));

            try (ResultSet result = statement.executeQuery()) {
                result.next();
                return result.getInt(1);
            }

        } catch (SQLException e) {
            throw new IllegalStateException(
                "Failed to count messages for agent: " + agent,
                e
            );
        }
    }

    @Override
    public boolean tryAddMessageWithinLimit(
        String agent,
        Instant now,
        int messageLimit
    ) {
        throw new UnsupportedOperationException(
            "PostgreSQL atomic insert is not implemented yet"
        );
    }
}


