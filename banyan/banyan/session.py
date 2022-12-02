from typing import List

from .constants import NOT_USING_MODULES
from .id import ResourceId, SessionId


sessions = {}
current_session_id = None
# Tasks for starting sessions
start_session_tasks = {}


class Session:
    """Stores information about one session"""

    def __init__(
        self,
        cluster_name: str,
        session_id: SessionId,
        resource_id: ResourceId,
        nworkers: int,
        organization_id: str = "",
        cluster_instance_id: str = "",
        not_using_modules: List[str] = NOT_USING_MODULES,
        is_cluster_ready: bool = False,
        is_session_ready: bool = False,
        scatter_queue_url: str = "",
        gather_queue_url: str = "",
        execution_queue_url: str = "",
        print_logs: bool = False,
    ):
        self.session_id = session_id
        self.resource_id = resource_id
        self.nworkers = nworkers
        self.locations = {}
        self.pending_requests = []
        self.futures_on_client = {}
        self.cluster_name = cluster_name
        self.worker_memory_used = 0
        self.organization_id = organization_id
        self.cluster_instance_id = cluster_instance_id
        self.not_using_modules: List[str] = not_using_modules
        self.loaded_packages: set[str] = set()
        self.is_cluster_ready = is_cluster_ready
        self.is_session_ready = is_session_ready
        self.scatter_queue_url = scatter_queue_url
        self.gather_queue_url = gather_queue_url
        self.execution_queue_url = execution_queue_url
        self.print_logs = print_logs


def sampling_configs_to_py(sampling_configs: Dict[LocationPath, SamplingConfig]):
    res = []
    for (l, s) in sampling_configs:
        res.append(
            (
                (l.original_path, l.format_name, l.format_version),
                (
                    s.rate,
                    s.always_exact,
                    s.max_num_bytes_exact,
                    s.force_new_sample_rate,
                    s.assume_shuffled,
                ),
            ),
        )
    return res


def sampling_configs_from_py(sampling_configs):
    res = {}
    for (l, s) in sampling_configs:
        res[LocationPath(l[0], l[1], l[2])] = SamplingConfig(
            s[0], s[1], s[2], s[3], s[4]
        )
    return res
