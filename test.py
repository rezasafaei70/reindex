import elasticsearch
from elasticsearch import Elasticsearch
es = Elasticsearch(["192.168.10.124:9201"], timeout=50)
query = {"source": {"index": "inews*", "size": 100, "remote": {"host": "http://192.168.10.110:9201"}, "query": {"range": {"info.first_packet_ts": {"gte": 1671325332, "lte": 1671325352, "boost": 2.0}}}}, "dest": {"index": "ddinews2022-12-18"}}
res = es.reindex(query)

print(res)