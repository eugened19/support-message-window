
package store;

import java.time.Instant;

public interface MessageWindowStore {

    // Read-only: count messages within the active window.
    int countActiveMessages(String agent, Instant now);

    // Atomic: check limit and record a message if allowed.
    // Expired messages must not count toward the limit.
    boolean tryAddMessageWithinLimit(
        String agent,
        Instant now,
        int messageLimit
    );
}


