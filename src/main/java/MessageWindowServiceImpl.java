
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MessageWindowServiceImpl implements MessageWindowService {

    private final Map<String, AgentWindow> windows =
        new ConcurrentHashMap<>();

    private final SlidingWindowCounter counter;
    private final int messageLimit;
    private final Clock clock;

    public MessageWindowServiceImpl(
        SlidingWindowCounter counter,
        int messageLimit,
        Clock clock
    ) {
        if (messageLimit <= 0) {
            throw new IllegalArgumentException(
                "Message limit must be positive"
            );
        }

        this.counter = counter;
        this.messageLimit = messageLimit;
        this.clock = clock;
    }

    @Override
    public boolean canAddMessage(AgentMessage agentMessage) {
        AgentWindow window = windows.get(agentMessage.agent());

        if (window == null) {
            return true;
        }

        window.lock.readLock().lock();

        try {
            Instant now = clock.instant();

            int count = counter.count(
                window.timestamps,
                now
            );

            return count < messageLimit;

        } finally {
            window.lock.readLock().unlock();
        }
    }

    @Override
    public void addMessage(String agent, String message) {
        AgentWindow window = windows.computeIfAbsent(
            agent,
            name -> new AgentWindow()
        );

        window.lock.writeLock().lock();

        try {
            Instant now = clock.instant();

            int count = counter.countAndRemoveExpired(
                window.timestamps,
                now
            );

            if (count >= messageLimit) {
                throw new MessageLimitExceededException(agent);
            }

            window.timestamps.addLast(now);

        } finally {
            window.lock.writeLock().unlock();
        }
    }

    private static class AgentWindow {

        private final Deque<Instant> timestamps =
            new ArrayDeque<>();

        private final ReentrantReadWriteLock lock =
            new ReentrantReadWriteLock();
    }
}
