"""Every Redis key Relay uses, in one place. See PROJECT_SPEC.md §6."""

from dataclasses import dataclass

from relay.models import Priority

GROUP = "relay-workers"  # the single consumer group every worker joins


@dataclass(frozen=True, slots=True)
class Keys:
    ns: str = "relay"

    def queue(self, priority: Priority | str) -> str:
        return f"{self.ns}:q:{priority}"

    @property
    def delayed(self) -> str:
        return f"{self.ns}:delayed"

    @property
    def delayed_meta(self) -> str:
        return f"{self.ns}:delayed:meta"

    @property
    def dlq(self) -> str:
        return f"{self.ns}:dlq"

    @property
    def workers(self) -> str:
        return f"{self.ns}:workers"

    def heartbeat(self, worker_id: str) -> str:
        return f"{self.ns}:hb:{worker_id}"

    def cancel(self, job_id: str) -> str:
        return f"{self.ns}:cancel:{job_id}"

    def lock(self, resource: str) -> str:
        return f"{self.ns}:lock:{resource}"

    def fence(self, resource: str) -> str:
        return f"{self.ns}:fence:{resource}"

    def type_slots(self, job_type: str) -> str:
        return f"{self.ns}:slots:{job_type}"

    @property
    def leader(self) -> str:
        return f"{self.ns}:leader:scheduler"

    def all_queues(self) -> list[str]:
        return [self.queue(p) for p in Priority]
