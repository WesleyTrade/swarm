class BaseAgent:
    def __init__(self, agent_id):
        self.agent_id = agent_id

    def run(self):
        raise NotImplementedError("Subclasses should implement this method.")

    def log_activity(self, message):
        print(f"[Agent {self.agent_id}] {message}")
