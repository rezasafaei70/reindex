import json
import logging
import math
import threading
import time

import elasticsearch
from elasticsearch import Elasticsearch

from models import Failure
from prometheus import count_reindex_kavosh, count_reindex_map, count_reindex_rtp, error_count_reindex_kavosh, \
    error_count_reindex_map, error_count_reindex_rtp
from settings import config
import datetime
from elasticsearch import helpers
from db import Session, engine, Base


logger = logging.getLogger('reindex')

Base.metadata.create_all(engine)

es = Elasticsearch([config['ELASTIC_DEST']],
                   timeout=int(config['TIMEOUT']), maxsize=100)
es1 = Elasticsearch([config['ELASTIC_SOURCE']],
                    timeout=int(config['TIMEOUT']), maxsize=100)  # request for  get the first time


lock = threading.Lock()


class Reindex:
    def __init__(self, index_name='', file_name=''):
        self.file_name = file_name
        if index_name != '':
            try:
                f = open(file_name, 'r')
                f.close()
                print("File Exists")
            except IOError:
                f = open(file_name, 'w+')
                f.close()
                print("File Created")

        self.index_name = index_name
        self.index_name_star = index_name + '*'

    def get_start_time(self, gte_time=0):
        info_time = self.read_file()
        logger.debug('Reindex:get_start_time before request is gte_time :' + str(gte_time) + " info_time:  "+ str(info_time))
        if len(info_time) == 0 or gte_time != 0:
            if self.index_name == config['MAP_INDEX']:
                sort = [{"indexed_time": {"order": "asc"}}]
                range = {"range": {"indexed_time": {
                    "gte": int(gte_time), "boost": 2.0}}}
            else:
                sort = [{"info.index_time": {"order": "asc"}}]
                range = {"range": {"info.index_time": {
                    "gte": int(gte_time), "boost": 2.0}}}

            query = {"size": 1, "sort": sort,
                     "query": range}
            try:
                res = es1.search(query, index=self.index_name_star)
                res = res['hits']['hits']
                if len(res):
                    if self.index_name == config['MAP_INDEX']:
                        info_time = res[0]['_source']['indexed_time']
                    else:
                        info_time = res[0]['_source']['info']['index_time']
                else:
                    logger.debug("Reindex:get_start_time not exists index")
                    return "0"
            except elasticsearch.ElasticsearchException as e:
                logger.error("Rindex:get_start_time  error exception:" + str(e))
                return "0"
        logger.debug("Reindex:get_start_time after request return info_time: " + str(info_time))
        return info_time

    def index(self):
        try:
            with lock:
                start_time = self.get_start_time()
                if start_time != '0':
                    logger.debug("index ------> start_time " + str(start_time))
                    end_time = int(start_time) + (int(config['ELASTIC_DURATION']) * 1000)
                    self.write_file(end_time)
                   
            time_now = int(time.time()) * 1000
            
            if int(start_time) < int(time_now) and start_time != '0':
                try:
                   
                    res = self.reindex(int(start_time),end_time)
                    logger.info("index -----> result " + json.dumps(res) +
                                " index_name " + self.index_name)
                  
                    if res['status'] == 'ok':
                        logger.info(
                            "index elastic reindex  created")
                    else:
                        with lock:
                            logger.debug(
                                'reindex ------> res[updated] and res[created] null')
                            s_time = self.get_start_time(start_time)
                            gte_time = self.read_file()
                            if int(s_time) > int(gte_time):
                                self.write_file(s_time)
                except Exception as e:
                    logger.error("index ----> index error"+str(e))        



                logger.debug(
                    "index ----> end_time write file " + str(end_time))
        except Exception as e:
            logger.warning("Reindex:reindex  " + str(e))

        return 'ok'

    def read_file(self):
        file = open(self.file_name, 'r')
        time = file.read()
        file.close()
        return time

    def write_file(self, end_time):
        try:
            if (end_time != "0"):
                file = open(self.file_name, 'w')
                file.write(str(end_time))
                file.close()

        except Exception as e:
            logger.error("write_file error " + str(e))

    def check_failure(self):
        try:
            session = Session()
            failurs = session.query(Failure).all()
            for item in failurs: 
                try:
                    index_name = item.index_name.split('*')[0]
                    query = json.loads(item.query)
                    res = es1.search(index=index_name,body=query)
                    arr = []
                    res_hits = res['hits']['hits']
                    if res_hits:
                        if res_hits:
                            for hits in res_hits:
                                del hits['_type']
                                arr.append(hits)   
                            res = helpers.bulk(es1,arr)
                    logger.info("check falure query " + json.dumps(query))
                    session.query(Failure).filter(
                        Failure.id == item.id).delete()
                    session.commit()
                    if self.index_name == config['KAVOSH_INDEX']:
                        count_reindex_kavosh(res['created'])
                    elif self.index_name == config['MAP_INDEX']:
                        count_reindex_map(res['created'])
                    elif self.index_name == config['RTP_INDEX']:
                        count_reindex_rtp(res['created'])
                except elasticsearch.ElasticsearchException as e:
                    logger.error("proplem index failure " +
                                 str(e) + "query " + json.dumps(query))
                    if self.index_name == config['KAVOSH_INDEX']:
                        error_count_reindex_kavosh(res['created'])
                    elif self.index_name == config['MAP_INDEX']:
                        error_count_reindex_map(res['created'])
                    elif self.index_name == config['RTP_INDEX']:
                        error_count_reindex_rtp(res['created'])

            Session.remove()
        except Exception as e:
            logger.error("Reindex:check_failure " + str(e))
        return "ok"
    
    def reindex(self,start_time,end_time):
        message = {"status":'none',"created":0}
        
        try:
            size = int(config['SIZE'])
            if self.index_name == config['MAP_INDEX']:
              
                range_array = {"range": {"indexed_time": {
                    "gte": start_time,"lte":end_time, "boost": 2.0}}}
            else:
                range_array = {"range": {"info.index_time": {
                    "gte": start_time,"lte":end_time, "boost": 2.0}}}

            query =   {"query": range_array}
            
            res = es1.search(index=self.index_name_star,body=query)
            logger.info("reindex "+ json.dumps(query))
            paginate = res['hits']['total']['value'] / size
            paginate = math.ceil(paginate)
            if paginate > 0 :
               
                for item in range(paginate):
                    if item !=0:
                        from_page = (item*size)+item
                    else :
                        from_page = item
                    if self.index_name == config['MAP_INDEX']:
                        range_array = {"range": {"indexed_time": {
                            "gte": start_time,"lte":end_time, "boost": 2.0}}}
                    else:
                        range_array = {"range": {"info.index_time": {
                            "gte": start_time,"lte":end_time, "boost": 2.0}}}
                        
                    query = {"size": size,"from": from_page,"query": range_array}
                    try:
                        res = es1.search(index=self.index_name_star,body=query) 

                        arr = []
                        res_hits = res['hits']['hits']
                        if res_hits:
                            if res_hits:
                                for hits in res_hits:
                                    del hits['_type']
                                    arr.append(hits)   
                        res = helpers.bulk(es,arr)
                        message['created'] =  message['created'] + res[0]         
                    except elasticsearch.ElasticsearchException as e:
                            logger.error("add error end time is :" +
                                        str(end_time) + "  index_name " + self.index_name)
                            logger.error(
                                "Reindex:reindex error elastic reindex " + str(e))

                            if self.index_name == config['KAVOSH_INDEX']:
                                error_count_reindex_kavosh(size)
                            elif self.index_name == config['MAP_INDEX']:
                                error_count_reindex_map(size)
                            elif self.index_name == config['RTP_INDEX']:
                                error_count_reindex_rtp(size)
                           
                            end_time = int(end_time) + (int(config['ELASTIC_DURATION']) * 1000)
                           
                            logger.info("Reindex:reindex after  write_file end_time :" +str(end_time) + "  index_name " + self.index_name)
                            session = Session()
                            failure = Failure(start_time=start_time, end_time=end_time,
                                            index_name=self.index_name_star, query=json.dumps(query))
                            session.add(failure)
                            session.commit()
                            Session.remove()
                message['status']='ok'
                return message
            else:
                message['status']='nok'
                return message
        except Exception as e:
            logger.error("reindex error "+str(e))
            message['status']='error'
            return message
              



