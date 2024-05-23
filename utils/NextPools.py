import os
from utils.auxiliar_functions import QUERY_SEPARATOR, get_env_list
from utils.QueryMessage import ALL_MESSAGE_FIELDS

GATEWAY_QUEUE_NAME = "Gateway"
NPW_POS = 0
SB_POS = 1

class NextPools():
    def __init__(self, next_pools_id, next_pool_workers, shard_by):
        self.pools = {}
        for i in range(len(next_pools_id)):
            self.pools[next_pools_id[i]] = (next_pool_workers[i], shard_by[i])
        
    @classmethod
    def from_env(cls):
        try:
            next_pool_workers = get_env_list('NEXT_POOL_WORKERS')
            next_pools_id = get_env_list("FORWARD_TO")
            shard_by = os.getenv("SHARD_BY").split(QUERY_SEPARATOR)
            for i in range(len(shard_by)):
                if shard_by[i] == '':
                    shard_by[i] = None
                elif not shard_by[i] in ALL_MESSAGE_FIELDS:
                    print(f"[Worker] Invalid Distribute_by field: {r}")
                    return None                
            return NextPools(next_pools_id, next_pool_workers, shard_by)
        except Exception as r:
            print(f"[Worker] Failed converting fowarding info: {r}")
            return None

    def worker_ids(self):
        worker_ids = {}
        for pool_id, next_pool_workers, _shard_by in self:
            if pool_id == GATEWAY_QUEUE_NAME:
                l = [GATEWAY_QUEUE_NAME]
            else:
                l = [f'{pool_id}.{j}' for j in range(int(next_pool_workers))]
            worker_ids[pool_id] = l
        return worker_ids
    
    def shard_by_of_pool(self, pool):
        return self.pools[pool][SB_POS]
    
    def __iter__(self):
        for pool, values in self.pools.items():
            yield pool, values[NPW_POS], values[SB_POS]