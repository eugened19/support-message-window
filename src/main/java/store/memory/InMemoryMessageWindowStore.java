
package store.memory;

import store.MessageWindowStore;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class InMemoryMessageWindowStore implements MessageWindowStore {

    private final Map<String, AgentWindow> windows =
        new ConcurrentHashMap<>();

    private final SlidingWindowCounter counter;

    public InMemoryMessageWindowStore(SlidingWindowCounter counter) {
        this.counter = counter;
    }

    @Override
    public int countActiveMessages(String agent, Instant now) {
        AgentWindow window = windows.get(agent);

        if (window == null) {
            return 0;
        }

        window.lock.readLock().lock();

        try {
            return counter.count(window.timestamps, now);
        } finally {
            window.lock.readLock().unlock();
        }
    }

    @Override
    public boolean tryAddMessageWithinLimit(
        String agent,
        Instant now,
        int messageLimit
    ) {
        AgentWindow window = windows.computeIfAbsent(
            agent,
            name -> new AgentWindow()
        );

        window.lock.writeLock().lock();

        try {
            int count = counter.countAndRemoveExpired(
                window.timestamps,
                now
            );

            if (count >= messageLimit) {
                return false;
            }

            window.timestamps.addLast(now);
            return true;

        } finally {
            window.lock.writeLock().unlock();
        }
    }

    private static class AgentWindow {
        private final Deque<Instant> timestamps = new ArrayDeque<>();
        private final ReentrantReadWriteLock lock =
            new ReentrantReadWriteLock();
    }
}


