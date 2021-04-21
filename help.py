import time
import random
from rediscluster.connection import ClusterConnectionPool
from rsacsc.manager import Manager
from rsacsc.client import Redis as CachedRedis
from rediscluster import RedisCluster
from rsacsc.client import RedisWithClient

startup_nodes = [{"host": "127.0.0.1", "port": "7000"}]

pool = ClusterConnectionPool(startup_nodes)
pool.nodes.initialize()
manager = Manager(pool=pool)

print(manager)
print(manager.client_id)


# cr = CachedRedis(manager)
#
# print("cr: ", cr)
# print(manager.slots)
# print(cr.get("foo"))
# print(cr.get("test"))
# print(manager.slots)
# print(cr.set("test", "from the python script"))
# print(manager.slots)
rc = RedisCluster(startup_nodes=startup_nodes, decode_responses=True)

rc.set("foo", "bar")
print(rc.get("foo"))
print(rc.get("test"))


cc = RedisWithClient(manager, rc)
print(cc.get("test"))

while True:
    print(manager.slots)
    print(cc.get("test"))
    time.sleep(10)
    v = random.randint(0, 10000000)
    print(f"set test value to: {v}")
    cc.client.set("test", v)
    time.sleep(10)
