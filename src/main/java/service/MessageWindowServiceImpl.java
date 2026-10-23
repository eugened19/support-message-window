
package service;

import domain.AgentMessage;
import domain.MessageLimitExceededException;
import store.MessageWindowStore;

import java.time.Clock;
import java.time.Instant;

public class MessageWindowServiceImpl implements MessageWindowService {

    private final MessageWindowStore store;
    private final int messageLimit;
    private final Clock clock;

    public MessageWindowServiceImpl(
        MessageWindowStore store,
        int messageLimit,
        Clock clock
    ) {
        if (messageLimit <= 0) {
            throw new IllegalArgumentException(
                "Message limit must be positive"
            );
        }

        this.store = store;
        this.messageLimit = messageLimit;
        this.clock = clock;
    }

    @Override
    public boolean canAddMessage(AgentMessage agentMessage) {
        Instant now = clock.instant();

        int count = store.countActiveMessages(
            agentMessage.agent(),
            now
        );

        return count < messageLimit;
    }

    @Override
    public void addMessage(String agent, String message) {
        Instant now = clock.instant();

        boolean added = store.tryAddMessageWithinLimit(
            agent,
            now,
            messageLimit
        );

        if (!added) {
            throw new MessageLimitExceededException(agent);
        }
    }
}
