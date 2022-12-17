import elasticsearch
from elasticsearch import Elasticsearch
es = Elasticsearch(["192.168.10.124:9201"], timeout=50)
query ={"source": {"index": "inews*", "size": 100, "remote": {"host": "http://192.168.10.110:9201"}, "query": {"range": {"info.index_time": {"gte": 1671264261008, "lte": 1671264291008, "boost": 2.0}}}}, "dest": {"index": "ddinews2022-12-17"}}

res = es.reindex(query)

print(res)