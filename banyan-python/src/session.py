class Session:
    """Stores information about one session
    """
    def _init_(self, cluster_name, session_id, resource_id, nworkers, sample_rate):
        self.cluster_name = cluster_name
        self.session_id = session_id
        self.resource_id = resource_id
        self.nworkers = nworkers
        self.sample_rate = sample_rate
        self.locations = {}
        self.pending_requests = []
        self.futures_on_client = {}

    @property
    def cluster_name(self):
        return self.cluster_name
    
    @property
    def job_id(self):
        return self.job_id
    
    @property
    def nworkers(self):
        return self.nworkers

    @property
    def sample_rate(self):
        return self.sample_rate
 