import elasticsearch
from elasticsearch import Elasticsearch
es = Elasticsearch(["192.168.10.124:9201"], timeout=50)
query ={'source': {'index': 'rtp-*', 'size': 100, 'remote': {'host': 'http://192.168.10.110:9201'}, 'query': {'range': {'info.first_packet_ts': {'gte': 1671327518, 'lte': 1671327568, 'boost': 2.0}}}}, 'dest': {'index': 'ddrtp-2022-12-18'}}
res = es.reindex(query)

print(res)