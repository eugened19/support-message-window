
import domain.AgentMessage;
import domain.MessageLimitExceededException;
import org.junit.jupiter.api.Test;
import service.MessageWindowService;
import service.MessageWindowServiceImpl;
import store.memory.InMemoryMessageWindowStore;
import store.memory.SlidingWindowCounter;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class MessageWindowServiceConcurrencyTest {

    private static final int MESSAGE_LIMIT = 5;
    private static final int THREAD_POOL_SIZE = 16;

    private MessageWindowService createService() {
        Clock clock = Clock.fixed(
            Instant.parse("2026-10-23T12:00:00Z"),
            ZoneOffset.UTC
        );

        return new MessageWindowServiceImpl(
            new InMemoryMessageWindowStore(
                new SlidingWindowCounter()
            ),
            MESSAGE_LIMIT,
            clock
        );
    }

    @Test
    void onlyFiveMessagesAcceptedUnderConcurrency() throws Exception {

        int attempts = 100;

        MessageWindowService service = createService();

        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();

        CountDownLatch startGate = new CountDownLatch(1);

        ExecutorService executor =
            Executors.newFixedThreadPool(THREAD_POOL_SIZE);

        List<Future<?>> futures = new ArrayList<>();

        try {
            for (int i = 0; i < attempts; i++) {
                futures.add(executor.submit(() -> {
                    startGate.await();

                    try {
                        service.addMessage("Alice", "Hello");
                        accepted.incrementAndGet();
                    } catch (MessageLimitExceededException e) {
                        rejected.incrementAndGet();
                    }

                    return null;
                }));
            }

            startGate.countDown();

            for (Future<?> future : futures) {
                future.get();
            }

            assertEquals(5, accepted.get());
            assertEquals(95, rejected.get());

            assertFalse(
                service.canAddMessage(
                    new AgentMessage("Alice", "Hello")
                )
            );

        } finally {
            startGate.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void multipleAgentsHaveIndependentLimits() throws Exception {

        int attemptsPerAgent = 100;

        String[] agents = {"Alice", "Bob", "Charlie"};

        MessageWindowService service = createService();

        Map<String, AtomicInteger> accepted = new ConcurrentHashMap<>();
        Map<String, AtomicInteger> rejected = new ConcurrentHashMap<>();

        for (String agent : agents) {
            accepted.put(agent, new AtomicInteger());
            rejected.put(agent, new AtomicInteger());
        }

        List<String> requests = new ArrayList<>();

        for (String agent : agents) {
            for (int i = 0; i < attemptsPerAgent; i++) {
                requests.add(agent);
            }
        }

        // Shuffle requests with a reproducible random seed.
        Collections.shuffle(requests, new Random(42));

        CountDownLatch startGate = new CountDownLatch(1);

        ExecutorService executor =
            Executors.newFixedThreadPool(THREAD_POOL_SIZE);

        List<Future<?>> futures = new ArrayList<>();

        try {
            for (String agent : requests) {
                futures.add(executor.submit(() -> {
                    startGate.await();

                    try {
                        service.addMessage(agent, "Hello");
                        accepted.get(agent).incrementAndGet();
                    } catch (MessageLimitExceededException e) {
                        rejected.get(agent).incrementAndGet();
                    }

                    return null;
                }));
            }

            startGate.countDown();

            for (Future<?> future : futures) {
                future.get();
            }

            for (String agent : agents) {
                assertEquals(
                    MESSAGE_LIMIT,
                    accepted.get(agent).get(),
                    agent + " accepted count"
                );

                assertEquals(
                    attemptsPerAgent - MESSAGE_LIMIT,
                    rejected.get(agent).get(),
                    agent + " rejected count"
                );

                assertFalse(
                    service.canAddMessage(
                        new AgentMessage(agent, "Hello")
                    )
                );
            }

        } finally {
            startGate.countDown();
            executor.shutdownNow();
        }
    }
}
