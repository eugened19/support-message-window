public interface MessageWindowService {

    void addMessage(String agent, String message);

    boolean canAddMessage(AgentMessage agentMessage);
}