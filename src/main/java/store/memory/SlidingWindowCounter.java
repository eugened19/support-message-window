package store.memory;

import java.time.Duration;
import java.time.Instant;
import java.util.Deque;

public class SlidingWindowCounter {

    private static final Duration WINDOW = Duration.ofMinutes(30);

    // Read-only: does not modify the queue.
    public int count(Deque<Instant> timestamps, Instant now) {
        Instant cutoff = now.minus(WINDOW);
        int count = 0;

        for (Instant timestamp : timestamps) {
            if (!timestamp.isBefore(cutoff)
                    && !timestamp.isAfter(now)) {
                count++;
            }
        }

        return count;
    }

    // Mutating: removes expired timestamps, then counts.
    public int countAndRemoveExpired(
        Deque<Instant> timestamps,
        Instant now
    ) {
        Instant cutoff = now.minus(WINDOW);

        while (!timestamps.isEmpty()
                && timestamps.peekFirst().isBefore(cutoff)) {
            timestamps.removeFirst();
        }

        return timestamps.size();
    }
}
