public class MessageLimitExceededException extends RuntimeException {

    public MessageLimitExceededException(String agent) {
        super("Message limit exceeded for agent: " + agent);
    }
}