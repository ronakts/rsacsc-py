"""
A local cache-aware Redis client
"""
from rediscluster import RedisCluster
from redis.exceptions import ResponseError


class Redis(RedisCluster):
    def client_kill_filter(self, _id=None, _type=None, addr=None, skipme=None):
        pass

    def __init__(self, manager, *args, **kwargs):
        super().__init__(self, *args, **kwargs)
        self._manager = manager
        self._client_id = super().client_id()
        self.execute_command('CLIENT', 'TRACKING', 'ON',
                             'redirect', self._manager.client_id)

    def close(self):
        self.execute_command('CLIENT', 'TRACKING', 'OFF')
        super().close()

    def get(self, name):
        try:
            value = self._manager.cache[name]
        except KeyError:
            value = super().get(name)
            self._manager.cache[name] = value
        return value


class RedisWithClient:
    def __init__(self, manager, cluster_client: RedisCluster):
        self._manager = manager
        self.client = cluster_client
        self._client_id = manager.client_id
        self._enable_client_tracking()

    def _enable_client_tracking(self):
        print(f"RedisWithClient: client ids: {self._client_id}")
        for cid in self._client_id:
            print(f"--------- client id: {cid}, value: {self._client_id[cid]}")
            try:
                print(f"Set tracking redirect to Client Id {self._client_id[cid]}")
                self.client.execute_command('CLIENT', 'TRACKING', 'ON', 'REDIRECT', str(self._client_id[cid]))
            except ResponseError as e:
                print(f"Client Id not found {self._client_id[cid]}, error: {str(e)}")

    def close(self):
        self.client.execute_command('CLIENT', 'TRACKING', 'OFF')
        self.client.close()

    def get(self, name):
        try:
            print("Try to get from cache")
            value = self._manager.cache[name]
        except KeyError:
            print("Missing key value in the cache, making call to redis cluster")
            value = self.client.get(name)
            print("Populate the cache")
            self._manager.cache[name] = value
        return value
