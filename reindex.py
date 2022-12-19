import json
import logging
import threading
import time

import elasticsearch
from elasticsearch import Elasticsearch

from models import Failure
from prometheus import count_reindex_kavosh, count_reindex_map, count_reindex_rtp, error_count_reindex_kavosh, \
    error_count_reindex_map, error_count_reindex_rtp
from settings import config
import datetime
from db import Session, engine, Base



logger = logging.getLogger('reindex')

Base.metadata.create_all(engine)

es = Elasticsearch([config['ELASTIC_DEST']], timeout=int(config['TIMEOUT']),maxsize=100)
es1 = Elasticsearch([config['ELASTIC_SOURCE']],
                    timeout=int(config['TIMEOUT']),maxsize=100)  # request for  get the first time
es3 = Elasticsearch([config['ELASTIC_DEST']], timeout=int(config['TIMEOUT'])*15)

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
        logger.debug('Reindex:get_start_time before request is gte_time :' + str(gte_time))
        logger.debug('Reindex:get_start_time before request is info_time:' + str(info_time))
        if len(info_time) == 0 or gte_time != 0:
            if self.index_name == config['MAP_INDEX']:
                sort = [{"indexed_time": {"order": "asc"}}]
                range = {"range": {"indexed_time": {"gte": int(gte_time), "boost": 2.0}}}
            else:
                sort = [{"info.first_packet_ts": {"order": "asc"}}]
                range = {"range": {"info.first_packet_ts": {"gte": int(gte_time), "boost": 2.0}}}
                
            query = {"size": 1, "sort": sort,
                         "query": range}
            try:
                
                res = es1.search(query, index=self.index_name_star)
                res = res['hits']['hits']
                if len(res):
                    if self.index_name == config['MAP_INDEX']:
                        info_time = res[0]['_source']['indexed_time']
                    else:
                        info_time = res[0]['_source']['info']['first_packet_ts']
                    logger.debug("Reindex:get_start_time after request info_time: " + str(info_time))
                else:
                    logger.debug("Reindex:get_start_time not exists index")

                    return "NOTEXIST"
            except elasticsearch.ElasticsearchException as e:
                logger.error("Rindex:get_start_time  error exception:" + str(e))
                logger.error("Rindex:get_start_time query "+ json.dumps(query))
                return "NOTEXIST"
        logger.debug("Reindex:get_start_time after request return info_time: " + str(info_time))
        return info_time

    def query(self, start_time, end_time,index_name):
        if index_name == config['MAP_INDEX']:
            range = {"range": {"indexed_time": {"gte": int(start_time), "lte": end_time, "boost": 2.0}}}
            time_dest = int(int(start_time) / 1000)
        else:
            range = {"range": {"info.first_packet_ts": {"gte": int(start_time), "lte": end_time, "boost": 2.0}}}
            time_dest = int(int(start_time))
        if config['SSL']=='TRUE':
            source = "https://"+config['ELASTIC_SOURCE']
        else:
            source = "http://"+config['ELASTIC_SOURCE']
        dest_index = "dd"+index_name + (datetime.datetime.fromtimestamp(time_dest).strftime('%Y-%m-%d'))
        logger.debug("REINDEX:query  dest_index : " + dest_index)
        query = {

            "source": {
                "index": index_name+"*",
                "size": int(config['SIZE']),
                "remote": {
                    "host": source,
                },
                "query": range
            },
            "dest": {
                "index": dest_index
            }
        }
        logger.debug("Reindex:query :" + json.dumps(query))
        return query

    def checktime(self, start_time, end_time):
        try:
            if self.index_name==config['MAP_INDEX']:
                start = datetime.datetime.fromtimestamp(int(int(start_time) / 1000))
                end = datetime.datetime.fromtimestamp(int(int(end_time) / 1000))
            else:
                start = datetime.datetime.fromtimestamp(int(int(start_time)))
                end = datetime.datetime.fromtimestamp(int(int(end_time))) 

            dest_index_name = 'dd'+self.index_name+end.strftime('%Y-%m-%d')
            source_index_name = self.index_name+end.strftime('%Y-%m-%d')
            try:
                if es.indices.exists(index=dest_index_name):
                    logger.debug("index_exist")
                else:
                    mapping = es1.indices.get_mapping(index=self.index_name_star)[source_index_name]['mappings']
                    settings_set = es1.indices.get_settings(index=self.index_name_star)[source_index_name]['settings']
                    del settings_set['index']['uuid']
                    del settings_set['index']['provided_name']
                    del settings_set['index']['version']
                    del settings_set['index']['creation_date']
            
                    body = {
                        "settings":settings_set,
                        "mappings":mapping
                    }
                    
                    es.indices.create(index=dest_index_name,body=body)
            except Exception as e:
                logger.error("Reindex:checktime create mapping error" + str(e)) 
            if start.day == end.day:
                return start_time, end_time
            else:
                if self.index_name==config['MAP_INDEX']:
                    end_time = int(datetime.datetime(year=start.year, month=start.month, day=start.day, hour=23, minute=59,
                                                second=59).timestamp()) * 1000
                else:
                    end_time = int(datetime.datetime(year=start.year, month=start.month, day=start.day, hour=23, minute=59,
                                                second=59).timestamp())
                return start_time, end_time
        except Exception as e:
            logger.error("Reindex:checktime error" + str(e)) 
            return 0, 0

    def reindex(self):
        try:
            with lock:
                start_time = self.get_start_time()
            if self.index_name == config['MAP_INDEX']:
                time_now = int(time.time()) * 1000
            else:
                time_now = int(time.time())
            if int(start_time) < int(time_now):
                if start_time != "NOTEXIST":
                    logger.debug("start_time " + str(start_time))
                    if self.index_name==config['MAP_INDEX']:
                        end_time = int(start_time) + (int(config['ELASTIC_DURATION']) * 1000)
                    else:
                        end_time = int(start_time) + int(config['ELASTIC_DURATION'])
                    with lock:
                        start_time, end_time = self.checktime(start_time, end_time)
                         
                    query = self.query(start_time, end_time,self.index_name)
                    try:
                        res = es.reindex(query)
                        logger.info("reindex result " + json.dumps(res) + " index_name " +self.index_name)
                        logger.info("reindex query" + str(query))
                        if res['updated'] or res['created']:
                            if self.index_name == config['KAVOSH_INDEX']:
                                count_reindex_kavosh(res['created'])
                            elif self.index_name == config['MAP_INDEX']:
                                count_reindex_map(res['created'])
                            elif self.index_name == config['RTP_INDEX']:
                                count_reindex_rtp(res['created'])

                            logger.debug("Reindex:reindex elastic reindex  created")
                            self.write_file(end_time)
                        else:
                            gte_time = self.read_file()
                            logger.debug('Reindex:reindex  res[updated] and res[created] null')
                            if len(gte_time) == 0:
                                gte_time = 0
                            s_time = self.get_start_time(gte_time)
                            self.write_file(s_time)

                    except elasticsearch.ElasticsearchException as e:
                        logger.error("add error end time is :" + str(end_time) + "  index_name " + self.index_name)
                        logger.error("Reindex:reindex error elastic reindex " + str(e))
                        logger.error("Reindex:reindex error elastic reindex query " + json.dumps(query))
                        if e.status_code == 'N/A':
                            if self.index_name == config['KAVOSH_INDEX']:
                                error_count_reindex_kavosh(res['created'])
                            elif self.index_name == config['MAP_INDEX']:
                                error_count_reindex_map(res['created'])
                            elif self.index_name == config['RTP_INDEX']:
                                error_count_reindex_rtp(res['created'])
                            logger.info("Reindex:reindex  conection failed  :" + str(end_time) + "  index_name " + self.index_name)
                        else:
                            logger.info("Reindex:reindex before write_file end_time :" + str(end_time) + "  index_name " + self.index_name)
                            if self.index_name == config['MAP_INDEX']:
                                end_time = int(end_time) + (int(config['ELASTIC_DURATION']) * 1000)
                            else:
                                end_time = int(end_time) + int(config['ELASTIC_DURATION'])
                            self.write_file(str(end_time))
                            logger.info("Reindex:reindex after  write_file end_time :" + str(end_time) + "  index_name " + self.index_name)
                            session = Session()
                            failure = Failure(start_time=start_time, end_time=end_time, index_name=self.index_name_star,query=json.dumps(query))
                            session.add(failure)
                            session.commit()
                            Session.remove()
                
                    logger.debug("Reindex:reindex end_time write file " + str(end_time))
        except Exception as e:
            logger.warning("Reindex:reindex  " + str(e))

        # self.reindex()
        return 'ok'

    def read_file(self):
        file = open(self.file_name, 'r')
        time = file.read()
        file.close()
        return time

    def write_file(self, end_time):
        try:
            with lock:
                if (end_time != "NOTEXIST"):
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
                index_name = item.index_name.split('*')[0]
                query = self.query(item.start_time, item.end_time,index_name)
                try:
                    res = es3.reindex(query)
                    logger.info("check falure " + json.dumps(res)+" index_name " +index_name)
                    logger.info("check falure query " + json.dumps(query))
                    session.query(Failure).filter(Failure.id == item.id).delete()
                    session.commit()
                    if self.index_name == config['KAVOSH_INDEX']:
                        count_reindex_kavosh(res['created'])
                    elif self.index_name == config['MAP_INDEX']:
                        count_reindex_map(res['created'])
                    elif self.index_name == config['RTP_INDEX']:
                        count_reindex_rtp(res['created'])
                except elasticsearch.ElasticsearchException as e:
                    logger.error("proplem index failure " + str(e) + "query "+ json.dumps(query))
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