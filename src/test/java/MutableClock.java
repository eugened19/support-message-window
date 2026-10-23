import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

public class MutableClock extends Clock {

    private Instant currentTime;

    public MutableClock(Instant initialTime) {
        this.currentTime = initialTime;
    }

    public void advance(Duration duration) {
        currentTime = currentTime.plus(duration);
    }

    @Override
    public Instant instant() {
        return currentTime;
    }

    @Override
    public ZoneId getZone() {
        return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return Clock.fixed(currentTime, zone);
    }
}
