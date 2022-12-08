from prometheus_client import Gauge
from elasticsearch import Elasticsearch

from settings import config

g = Gauge('count_reindex_kavosh', 'number of reindex kavaosh')
g1 = Gauge('count_reindex_map', 'number of reindex map')
g2 = Gauge('count_reindex_rtp', 'number of reindex rtp')

g3 = Gauge('error_reindex_kavosh', 'number of error kavosh')
g4 = Gauge('error_reindex_map', 'number of error map')
g5 = Gauge('error_reindex_rtp', 'number of error rtp')
g6 = Gauge('count_from_kavosh', 'number of origin index kavosh')
g7 = Gauge('count_from_map', 'number of origin index map')
g8 = Gauge('count_from_rtp', 'number of origin index rtp')


g9 = Gauge('count_dest_kavosh', 'number remote reindex kavosh')
g10 = Gauge('count_dest_map', 'number of remote reindex index map')
g11 = Gauge('count_dest_rtp', 'number of remote reindex rtp')

es = Elasticsearch([config['ELASTIC_SOURCE']], timeout=int(config['TIMEOUT']))
es1 = Elasticsearch([config['ELASTIC_DEST']], timeout=int(config['TIMEOUT']))


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
def count_from_kavosh():
    count = es.count(index=config['KAVOSH_INDEX']+"*", body={'query': {'match_all':{}}})["count"]
    g6.set(count)

@g7.track_inprogress()
def count_from_map():
    count = es.count(index=config['MAP_INDEX']+"*", body={'query': {'match_all':{}}})["count"]
    g7.set(count)

@g8.track_inprogress()
def count_from_rtp():
    count = es.count(index=config['RTP_INDEX']+"*", body={'query': {'match_all':{}}})["count"]
    g8.set(count)
    
    
@g9.track_inprogress()
def count_dest_kavosh():
    count = es1.count(index=config['KAVOSH_INDEX']+"*", body={'query': {'match_all':{}}})["count"]
    g9.set(count)

@g10.track_inprogress()
def count_dest_map():
    count = es1.count(index=config['MAP_INDEX']+"*", body={'query': {'match_all':{}}})["count"]
    g10.set(count)

@g11.track_inprogress()
def count_dest_rtp():
    count = es1.count(index=config['RTP_INDEX']+"*", body={'query': {'match_all':{}}})["count"]
    g11.set(count)

