from dataclasses import dataclass


@dataclass
class SessionInfo:
    """Information about a particular session."""
    session_name: str
    session_id: str
    num_workers: int
    scatter_queue_url: str
    gather_queue_url: str
