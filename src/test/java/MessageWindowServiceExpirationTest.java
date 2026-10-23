
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MessageWindowServiceExpirationTest {

    @Test
    void messagesExpireAfterThirtyMinutes() {

        MutableClock clock = new MutableClock(
            Instant.parse("2026-10-23T12:00:00Z")
        );

        MessageWindowService service = new MessageWindowServiceImpl(
            new SlidingWindowCounter(),
            5,
            clock
        );

        AgentMessage alice = new AgentMessage("Alice", "Hello");

        // 12:00 — Fill Alice's window.
        for (int i = 0; i < 5; i++) {
            service.addMessage("Alice", "Hello");
        }

        assertFalse(service.canAddMessage(alice));

        assertThrows(
            MessageLimitExceededException.class,
            () -> service.addMessage("Alice", "Sixth message")
        );

        // 12:29 — All five messages still count.
        clock.advance(Duration.ofMinutes(29));

        assertFalse(service.canAddMessage(alice));

        // 12:30 exactly — The cutoff is inclusive.
        clock.advance(Duration.ofMinutes(1));

        assertFalse(service.canAddMessage(alice));

        // 12:30 + 1 nanosecond — All five messages expire.
        clock.advance(Duration.ofNanos(1));

        assertTrue(service.canAddMessage(alice));

        // addMessage() removes expired timestamps before inserting.
        assertDoesNotThrow(
            () -> service.addMessage("Alice", "New message")
        );

        // One active message, four available slots.
        assertTrue(service.canAddMessage(alice));

        // Fill the remaining four slots.
        for (int i = 0; i < 4; i++) {
            service.addMessage("Alice", "Another message");
        }

        assertFalse(service.canAddMessage(alice));
    }
}
