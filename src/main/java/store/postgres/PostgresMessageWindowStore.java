
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
        Instant cutoff = now.minus(WINDOW);

        try (Connection connection = dataSource.getConnection()) {

            // TRANSACTION vs LOCK
            // -------------------
            // setAutoCommit(false) enables manual transaction control.
            // PostgreSQL begins the transaction when the first SQL statement executes.
            //
            // All SQL statements executed through THIS connection participate
            // in the same transaction until commit() or rollback().
            //
            // A transaction groups operations into one atomic unit of work.
            // A lock controls how concurrent transactions interact.
            //
            // SELECT FOR UPDATE does NOT create the transaction.
            connection.setAutoCommit(false);

            try {
                long agentId;

                // STEP 1: PESSIMISTIC ROW LOCKING
                // -------------------------------
                // SELECT FOR UPDATE locks the selected agent row.
                //
                // WHY LOCK agents INSTEAD OF agent_messages?
                //
                // The agent row always exists, even when there are zero messages.
                // Locking existing message rows would not protect the empty case.
                //
                // The agent row acts as our synchronization point.
                //
                // If two requests arrive for the SAME agent:
                //   Thread A: acquires the lock.
                //   Thread B: waits for Thread A to commit or roll back.
                //
                // Requests for DIFFERENT agents can proceed concurrently.
                //
                // IMPORTANT:
                // This works only if every message writer follows
                // the same locking protocol.
                //
                // FOR UPDATE does not block ordinary SELECT queries.
                // It blocks conflicting row locks and modifications.
                try (PreparedStatement statement = connection.prepareStatement("""
                        SELECT agent_id
                        FROM agents
                        WHERE agent_name = ?
                        FOR UPDATE
                        """)) {

                    statement.setString(1, agent);

                    try (ResultSet result = statement.executeQuery()) {
                        if (!result.next()) {
                            throw new IllegalArgumentException(
                                    "Unknown agent: " + agent
                            );
                        }

                        agentId = result.getLong("agent_id");
                    }
                }

                // STEP 2: COUNT MESSAGES INSIDE THE ROLLING WINDOW
                // ------------------------------------------------
                // We count only messages within [now - WINDOW, now].
                //
                // WHY IS THIS SAFE AFTER SELECT FOR UPDATE?
                //
                // PostgreSQL's default isolation level is READ COMMITTED.
                // Each SQL statement gets a fresh snapshot of committed data.
                //
                // Example: limit = 10, current count = 9.
                //
                //   Thread A locks agent, counts 9, inserts #10, commits.
                //   Thread B waits for the lock.
                //   Thread B acquires lock, counts 10, rejects.
                //
                // Without the lock, both threads could count 9
                // and both insert, exceeding the limit.
                //
                // The lock protects the entire CHECK + INSERT sequence.
                int count;

                try (PreparedStatement statement = connection.prepareStatement("""
                        SELECT COUNT(*)
                        FROM agent_messages
                        WHERE agent_id = ?
                        AND received_at >= ?
                        AND received_at <= ?
                        """)) {

                    statement.setLong(1, agentId);
                    statement.setTimestamp(2, Timestamp.from(cutoff));
                    statement.setTimestamp(3, Timestamp.from(now));

                    try (ResultSet result = statement.executeQuery()) {
                        result.next();
                        count = result.getInt(1);
                    }
                }

                // STEP 3: ENFORCE THE LIMIT
                // -------------------------
                // The agent row is STILL LOCKED here.
                //
                // Closing PreparedStatement or ResultSet does not release
                // the transaction's row lock.
                //
                // If the limit is reached, finish the transaction normally.
                // Even though we made no changes, commit() releases the lock.
                if (count >= messageLimit) {
                    connection.commit();
                    return false;
                }

                // STEP 4: INSERT THE MESSAGE
                // --------------------------
                // The INSERT executes on the SAME connection and transaction.
                //
                // Therefore:
                //   SELECT FOR UPDATE
                //   SELECT COUNT(*)
                //   INSERT
                //
                // are all part of ONE transaction.
                //
                // PreparedStatement uses parameterized raw SQL.
                // The ? placeholders prevent SQL injection and allow
                // JDBC to bind values using the appropriate SQL types.
                try (PreparedStatement statement = connection.prepareStatement("""
                        INSERT INTO agent_messages (agent_id, received_at)
                        VALUES (?, ?)
                        """)) {

                    statement.setLong(1, agentId);
                    statement.setTimestamp(2, Timestamp.from(now));
                    statement.executeUpdate();
                }

                // STEP 5: COMMIT
                // --------------
                // Commit makes the INSERT permanent and visible to
                // subsequent READ COMMITTED statements.
                //
                // It also releases the agent row lock.
                //
                // Only now can another waiting request for this agent
                // acquire the lock and perform its own COUNT + INSERT.
                connection.commit();
                return true;

            } catch (SQLException | RuntimeException e) {

                // ROLLBACK
                // --------
                // If any operation fails, undo all uncommitted changes
                // and release transaction locks.
                //
                // Example:
                //   SELECT FOR UPDATE succeeds.
                //   COUNT succeeds.
                //   INSERT fails.
                //
                // ROLLBACK ensures no partial transaction is committed.
                //
                // Preserve the original exception if rollback also fails.
                try {
                    connection.rollback();
                } catch (SQLException rollbackError) {
                    e.addSuppressed(rollbackError);
                }

                throw e;
            }

        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed to insert message for agent: " + agent,
                    e
            );
        }
    }
}


