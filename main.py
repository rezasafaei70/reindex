
import datetime
import os
import threading
import time
from os.path import exists
from prometheus_client import start_http_server

from prometheus import count_dest_kavosh, count_dest_map, count_dest_rtp, count_from_kavosh, count_from_map, count_from_rtp
from reindex import Reindex
import logging
import logging.handlers as handlers
from settings import config

logging.basicConfig()
logging.getLogger('sqlalchemy').setLevel(logging.ERROR)

logging.getLogger('elasticsearch').setLevel(logging.ERROR)
logger = logging.getLogger('reindex')
logger.setLevel(eval(config['LOGLEVEL']))

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logHandler = handlers.TimedRotatingFileHandler('logs/debug.log', when='d', interval=1, backupCount=5)
# logHandler.setLevel(logging.INFO)

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
        reindex_kavosh.index()
        time.sleep(int(config['TIME_SLEEP']))
def reindex_rtp():
    reindex_rtp = Reindex(config['RTP_INDEX'], 'temp/rtp')
    while True:
        reindex_rtp.index()
        time.sleep(int(config['TIME_SLEEP']))

def reindex_map():
    reindex_map = Reindex(config['MAP_INDEX'], 'temp/map')
    while True:
        reindex_map.index()
        time.sleep(int(config['TIME_SLEEP']))

def failure():
    reindex_failure = Reindex()
    while True:
        reindex_failure.check_failure()
        time.sleep(int(config['TIME_SLEEP']))
if __name__ == '__main__':
    start_http_server(8000)
    time_index = config['time_index']
    if config['time_index']:
        start_time_path = 'temp/start_time'
        file_exists = os.path.exists(start_time_path)
        time_format = '{time_index} 00:00:01'.format(time_index=time_index)
        set_time = datetime.datetime.strptime(time_format,'%d-%m-%Y %H:%M:%S')
        millisec = str(int(set_time.timestamp() * 1000))
        if not file_exists:
            f = open(start_time_path, 'w+')
            f.close()
        f = open(start_time_path,'r')
        start_time_file = f.read()
        if start_time_file != millisec:
            file = open(start_time_path, 'w+')
            file.write(millisec)
            file.close()
            file = open('temp/index','w')
            file.write(millisec)
            file.close()
            file = open('temp/rtp','w')
            file.write(millisec)
            file.close()
            file = open('temp/map','w')
            file.write(millisec)
            file.close()
        

    thread_koavosh = []
    for i in range(int(config['THREADS'])):
        t1 = threading.Thread(target=reindex_kavosh)
        thread_koavosh.append(t1)
    for t1 in thread_koavosh:
        t1.start()
    
    # thread_map = []
    # for i in range(int(config['THREADS'])):
    #     t2 = threading.Thread(target=reindex_map)
    #     thread_map.append(t2)
    # for t2 in thread_map:
    #     t2.start()
    
    # thread_rtp = []
    # for i in range(int(config['THREADS'])):
    #     t3 = threading.Thread(target=reindex_rtp)
    #     thread_rtp.append(t3)
    # for t3 in thread_rtp:
    #     t3.start()
        

    t4 = threading.Thread(target=failure)
    t4.start()
   
    check_index_origin()














