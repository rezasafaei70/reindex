from prometheus_client import Gauge
from elasticsearch import Elasticsearch

from settings import config

g = Gauge('count_reindex_kavosh', 'number of reindex kavaosh')
g1 = Gauge('count_reindex_map', 'number of reindex map')
g2 = Gauge('count_reindex_rtp', 'number of reindex rtp')

g3 = Gauge('error_reindex_kavosh', 'number of error kavosh')
g4 = Gauge('error_reindex_map', 'number of error map')
g5 = Gauge('error_reindex_rtp', 'number of error rtp')
g6 = Gauge('count_index_origin_kavosh', 'number of origin index kavosh')
g7 = Gauge('count_index_origin_map', 'number of origin index map')
g8 = Gauge('count_index_origin_rtp', 'number of origin index rtp')

es = Elasticsearch([config['ELASTIC_HOST_REQUEST_TIME']], timeout=int(config['TIMEOUT']))

@g.track_inprogress()
def count_reindex_kavosh(value):
    g.inc(value)

@g1.track_inprogress()
def count_reindex_map(value):
    g1.inc(value)

@g2.track_inprogress()
def count_reindex_rtp(value):
    g2.inc(value)

@g3.track_inprogress()
def error_count_reindex_kavosh(value):
    g3.inc(value)
@g4.track_inprogress()
def error_count_reindex_map(value):
    g4.inc(value)
@g5.track_inprogress()
def error_count_reindex_rtp(value):
    g5.inc(value)

@g6.track_inprogress()
def count_origin_index_kavosh():
    count = es.count(index=config['KAVOSH_INDEX']+"*", body={'query': {'match_all':{}}})["count"]
    g6.set(count)

@g7.track_inprogress()
def count_origin_index_map():
    count = es.count(index=config['MAP_INDEX']+"*", body={'query': {'match_all':{}}})["count"]
    g7.set(count)

@g8.track_inprogress()
def count_origin_index_rtp():
    count = es.count(index=config['RTP_INDEX']+"*", body={'query': {'match_all':{}}})["count"]
    g8.set(count)

