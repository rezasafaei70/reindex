import elasticsearch
from elasticsearch import Elasticsearch
es = Elasticsearch(["192.168.10.124:9201"], timeout=50)
query ={"source": {"index": "inews*", "size": 10, "remote": {"host": "http://192.168.10.110:9201"}, "query": {"range": {"info.index_time": {"gte": 1671339052645, "lte": 1671339062645, "boost": 2.0}}}}, "dest": {"index": "ddinews2022-12-18"}}
res = es.reindex(query)

print(res)