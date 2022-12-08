
import threading
import time

from prometheus_client import start_http_server

from prometheus import count_dest_kavosh, count_dest_map, count_dest_rtp, count_from_kavosh, count_from_map, count_from_rtp
from reindex import Reindex
import logging
import logging.handlers as handlers
from settings import config


logger = logging.getLogger('reindex')
logger.setLevel(eval(config['LOGLEVEL']))

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logHandler = handlers.TimedRotatingFileHandler('logs/debug.log', when='d', interval=1, backupCount=5)
logHandler.setLevel(logging.INFO)

logHandler.setFormatter(formatter)

logger.addHandler(logHandler)


def check_index_origin():
    while True:
        count_from_rtp()
        count_from_kavosh()
        count_from_map()
        count_dest_rtp()
        count_dest_kavosh()
        count_dest_map()
        time.sleep(int(config['TIME_SLEEP']))

def reindex_kavosh():
    reindex_kavosh = Reindex(config['KAVOSH_INDEX'], 'temp/index')
    while True:
        reindex_kavosh.reindex()
        time.sleep(int(config['TIME_SLEEP']))
def reindex_rtp():
    reindex_rtp = Reindex(config['RTP_INDEX'], 'temp/rtp')
    while True:
        reindex_rtp.reindex()
        time.sleep(int(config['TIME_SLEEP']))

def reindex_map():
    reindex_map = Reindex(config['MAP_INDEX'], 'temp/map')
    while True:
        reindex_map.reindex()
        time.sleep(int(config['TIME_SLEEP']))

def failure():
    reindex_failure = Reindex()
    while True:
        reindex_failure.check_failure()
        time.sleep(int(config['TIME_SLEEP']))
if __name__ == '__main__':
    start_http_server(8000)
    t1 = threading.Thread(target=reindex_kavosh)
    t1.start()
    t2 = threading.Thread(target=reindex_map)
    t2.start()
    t3 = threading.Thread(target=reindex_rtp)
    t3.start()
    t4 = threading.Thread(target=failure)
    t4.start()
    
    check_index_origin()














