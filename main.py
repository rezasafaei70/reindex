
import threading
import time

from prometheus_client import start_http_server

from prometheus import count_origin_index_map, count_origin_index_kavosh, count_origin_index_rtp
from reindex import Reindex
import logging
from settings import config

logging.basicConfig(filename='logs/debug.log', encoding='utf-8', level=eval(config['LOGLEVEL']),
                    format='%(asctime)s %(levelname)s:%(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
logging.getLogger('elasticsearch').setLevel(logging.ERROR)


def check_index_origin():
    count_origin_index_rtp()
    count_origin_index_kavosh()
    count_origin_index_map()
    time.sleep(500)
    check_index_origin()

if __name__ == '__main__':
    start_http_server(8000)
    reindex_kavosh = Reindex(config['KAVOSH_INDEX'], 'temp/index')
    reindex_map = Reindex(config['MAP_INDEX'], 'temp/map')
    reindex_rtp = Reindex(config['RTP_INDEX'], 'temp/rtp')

    thread_index_origin = threading.Thread(target=check_index_origin)
    thread_index_origin.start()
    #
    thread_kavosh = threading.Thread(target=reindex_kavosh.reindex)
    thread_kavosh.start()

    thread_map = threading.Thread(target=reindex_map.reindex)
    thread_map.start()

    thread_rtp = threading.Thread(target=reindex_rtp.reindex)
    thread_rtp.start()

    check_failure = threading.Thread(target=reindex_rtp.check_failure)
    check_failure.start()










