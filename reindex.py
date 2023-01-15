import datetime
import json
import logging
import math
import re
import threading
import time
import requests
from elasticsearch import Elasticsearch

from models import Failure
from prometheus import count_reindex_kavosh, count_reindex_map, count_reindex_rtp, error_count_reindex_kavosh, \
    error_count_reindex_map, error_count_reindex_rtp
from settings import config
from holder import get_check_index, get_status, set_check_index, set_status
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

        logger.debug('Reindex:get_start_time before request is gte_time :' + str(
            gte_time) + " info_time:  " + str(info_time) + " index_name " + self.index_name)
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
                logger.debug("get_start_time send request time query is " +
                            json.dumps(query)+" index_name " + self.index_name_star)

                res = es1.search(query, index=self.index_name_star)
               
                res = res['hits']['hits']
                if len(res):
                    if self.index_name == config['MAP_INDEX']:
                        info_time = res[0]['_source']['indexed_time']
                    else:
                        info_time = res[0]['_source']['info']['index_time']
                else:
                    logger.debug("Reindex:get_start_time not exists index name is " +
                                self.index_name_star + " query is " + json.dumps(query))
                    return 0
            except Exception as e:
                logger.error("Rindex:get_start_time  error exception:" +
                             str(e) + "i ndex_name " + self.index_name_star)
                return -1
        logger.debug("Reindex:get_start_time after request return info_time: " +
                     str(info_time) + " index_name " + self.index_name_star)
        return info_time

    def set_settings(self, end_time):
        with lock:
            end = datetime.datetime.fromtimestamp(int(int(end_time) / 1000))
            dest_index_name = self.index_name+end.strftime('%Y-%m-%d')
            source_index_name = self.index_name+end.strftime('%Y-%m-%d')
            if get_check_index() != dest_index_name:
                try:
                    if es.indices.exists(index=dest_index_name):
                        logger.debug("index_exist")
                    else:
                        mapping = es1.indices.get_mapping(index=self.index_name_star)[
                            source_index_name]['mappings']
                        settings_set = es1.indices.get_settings(index=self.index_name_star)[
                            source_index_name]['settings']
                        del settings_set['index']['uuid']
                        del settings_set['index']['provided_name']
                        del settings_set['index']['version']
                        del settings_set['index']['creation_date']

                        body = {
                            "settings": settings_set,
                            "mappings": mapping
                        }

                        es.indices.create(index=dest_index_name, body=body)
                        set_check_index(dest_index_name)
                except Exception as e:
                    logger.error("Reindex:checktime create mapping error" + str(e))
        

    def read_file(self):
        file = open(self.file_name, 'r')
        time = file.read()
        file.close()
        return time

    def write_file(self, end_time):
        try:
            time_now = int(time.time()) * 1000
            if (end_time != '0' and end_time < time_now):
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
                    index_name = item.index_name
                    query = json.loads(item.query)
                    res = es1.search(index=index_name, body=query)
                    res_hits = res['hits']['hits']
                   
                    if res_hits:
                        if res_hits:
                            for hits in res_hits:
                                del hits['_type']
                               
                                del hits['_source']['info']['head']
                                try:
                                    if 'data' == hits['_source']:
                                        hits['_source']['data'] = removeWeirdChars(hits['_source']['data'])
                                    res = helpers.bulk(es, [hits],refresh=True)
                                    logger.info("failure succses removeWeirdChars and index res is " )
                                    session.query(Failure).filter(Failure.id == item.id).delete()
                                    session.commit()
                                    Session.remove()
                                except Exception as e:
                                    try:
                                        if 'data' in  hits['_source']:   
                                            del hits['_source']['data']
                                            res = helpers.bulk(es, [hits],refresh=True)
                                            logger.info("failure succses del data and index")
                                        else:
                                            logger.error(" not index failure " +str(e)+" index_name "+self.index_name)
                                    except Exception as e :

                                        logger.error("not index failure " +str(e))
                                   
                    logger.debug("check falure query " +
                                json.dumps(query) + " index_name " + index_name)
                   
                    if self.index_name == config['KAVOSH_INDEX']:
                        count_reindex_kavosh(res['created'])
                    elif self.index_name == config['MAP_INDEX']:
                        count_reindex_map(res['created'])
                    elif self.index_name == config['RTP_INDEX']:
                        count_reindex_rtp(res['created'])
                except Exception as e:
                    logger.error("proplem index failure " +
                                 str(e) + "query " + json.dumps(query) + " index_name " + item.index_name)
                    if self.index_name == config['KAVOSH_INDEX']:
                        error_count_reindex_kavosh(res['created'])
                    elif self.index_name == config['MAP_INDEX']:
                        error_count_reindex_map(res['created'])
                    elif self.index_name == config['RTP_INDEX']:
                        error_count_reindex_rtp(res['created'])

            
        except Exception as e:
            logger.error("Reindex:check_failure " + str(e))
        return "ok"

    def index(self):
        try:
            time_now = int(time.time()) * 1000
            step_time = int(config['ELASTIC_DURATION'])*600000
            with lock:
               
                
                start_time = self.get_start_time()
                logger.debug("index ------> start_time " +
                            str(start_time) + " index_name "+self.index_name)
 

                if int(start_time) > 0 and int(start_time)+step_time < int(time_now):
                    end_time = int(start_time) + \
                        (int(config['ELASTIC_DURATION']) * 1000)
                    self.write_file(end_time)

           
            if int(start_time) > 0:
                if int(start_time)+step_time < int(time_now):
                    try:
                        self.set_settings(end_time)
                        res = self.reindex(int(start_time), end_time)

                        if res['status'] == 'ok':
                            logger.info("index -----> result created " + json.dumps(res) +
                                        " index_name " + self.index_name)
                       
                        else:
                            with lock:
                                s_time = self.get_start_time(start_time)
                                gte_time = self.read_file()
                              
                                logger.info('reindex ------> res[updated] and res[created] null index_name '
                                            + self.index_name + "start_time " +
                                            str(start_time) + " end_time " +
                                            str(end_time) + " gte_time " +
                                            str(gte_time)
                                            + " s_time " + str(s_time))
                                if int(s_time) > int(gte_time):
                                    end_time = int(
                                        s_time) + (int(config['ELASTIC_DURATION']) * 1000)
                                    self.write_file(s_time)
                                else:
                                    
                                    logger.debug(
                                        "stime < gte_time "+" s_time " + str(s_time)+" gte_time "+str(gte_time))
                    except Exception as e:
                        logger.error("index ----> index error "+str(e)+" index_name " + self.index_name +
                                    " start_time " + str(start_time) + " end_time " + str(end_time))

                    logger.debug(
                        "index ----> end_time write file "+" index_name " + self.index_name+" start_time " + str(start_time) + " end_time " + str(end_time))
                else:
                    time.sleep(int(config['TIME_SLEEP']))
        except Exception as e:
            logger.warning("Reindex:reindex  " + str(e) +
                           " index_name " + self.index_name)

        return 'ok'

    def reindex(self, start_time, end_time):
        message = {"status": 'none', "created": 0}
        try:
            size = int(config['SIZE'])
            if self.index_name == config['MAP_INDEX']:

                range_array = {"range": {"indexed_time": {
                    "gte": start_time, "lte": end_time, "boost": 2.0}}}
            else:
                range_array = {"range": {"info.index_time": {
                    "gte": start_time, "lte": end_time, "boost": 2.0}}}

            query = {"query": range_array}
            query1 = {}
            res = es1.search(index=self.index_name_star, body=query)
           
            
            paginate = res['hits']['total']['value'] / size
            logger.debug("before reindex log query and count " + json.dumps(query) + " count " + str(res['hits']['total']['value']))
            paginate = math.ceil(paginate)
            
            if paginate > 0:
          
                for item in range(paginate):
                    if item != 0:
                        from_page = (item*size)
                    else:
                        from_page = item
                    if self.index_name == config['MAP_INDEX']:
                        range_array = {"range": {"indexed_time": {
                            "gte": start_time, "lte": end_time, "boost": 2.0}}}
                    else:
                        range_array = {"range": {"info.index_time": {
                            "gte": start_time, "lte": end_time, "boost": 2.0}}}

                    query1 = {"size": size, "from": from_page,
                             "query": range_array}
                    try:
                        res2 = es1.search(
                            index=self.index_name_star, body=query1)

                        arr = []
                        res_hits = res2['hits']['hits']
                 
                        if res_hits:
                            if res_hits:
                                for hits in res_hits:
                                    del hits['_type']
                                    arr.append(hits)
                        if len(arr)>0:
                            res1 = helpers.bulk(es, arr, refresh=True)
                            message['created'] = message['created'] + res1[0]
                            message['status'] = 'ok'
                        else:
                            message['status'] = 'nok'
                    except Exception as e:
                        message['status'] = 'ok'
                        logger.error("reindex error elastic  :" + str(e) + "  index_name " +
                                     self.index_name + " query " + json.dumps(query1))
                        if self.index_name == config['KAVOSH_INDEX']:
                            error_count_reindex_kavosh(size)
                        elif self.index_name == config['MAP_INDEX']:
                            error_count_reindex_map(size)
                        elif self.index_name == config['RTP_INDEX']:
                            error_count_reindex_rtp(size)
                        end_time = int(end_time) + \
                            (int(config['ELASTIC_DURATION']) * 1000)
                        logger.debug("Reindex:reindex after  write_file end_time :" + str(
                            end_time) + "  index_name " + self.index_name + " query " + json.dumps(query1))
                        session = Session()
                        failure = Failure(start_time=start_time, end_time=end_time,
                                          index_name=self.index_name_star, query=json.dumps(query1))
                        session.add(failure)
                        session.commit()
                        Session.remove()
                        
                try:
                    count = res['hits']['total']['value']
                    res3 = es1.search(index=self.index_name_star, body=query)
                    
                    count3 = res3['hits']['total']['value']
                    if int(message['created']) != int(count3) or int(count3)==10000:
                        logger.error("after reindex log query and count " + json.dumps(query) + " count " + str(res['hits']['total']['value']))
                        logger.error("ERROR NOT equlas index count is "+ str(count) + " created is "+str(message['created']) +" last count is " +str(count3) + " start time " +str(start_time) + " end_time " + str(end_time))              
                except Exception as e:
                    logger.error("count error " + str(e))
               
                return message
            else:
                message['status'] = 'nok'
                return message
        except Exception as e:
            logger.error("reindex error "+str(e))
            message['status'] = 'error'
            return message


def removeWeirdChars(text):
    weridPatterns = re.compile("["
                               u"\U0001F600-\U0001F64F"  # emoticons
                               u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                               u"\U0001F680-\U0001F6FF"  # transport & map symbols
                               u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                               u"\U00002702-\U000027B0"
                               u"\U000024C2-\U0001F251"
                               u"\U0001f926-\U0001f937"
                               u'\U00010000-\U0010ffff'
                               u"\u200d"
                               u"\u2640-\u2642"
                               u"\u2600-\u2B55"
                               u"\u23cf"
                               u"\u23e9"
                               u"\ude67"
                               u"\uda14"
                               u"\ud81f"
                               u"\udb97"
                               u"\udb57"
                               u"\uda14"
                               u"\ude15"
                               u"\u231a"
                               u"\u3030"
                               u"\ufe0f"
                               u"\udd00"
                               u"\udbba"
                               u"\uda4f"
                               u"\ude82"
                               u"\udce7"
                               u"\uda01"
                               u"\udc94"
                               u"\u2069"
                               u"\u2066"
                               u"\u200c"
                               u"\u2068"
                               u"\u2067"
                               u"\uda64"
                               u"\ud9de"
                               u"\uda42"
                               u"\udd2b"
                               u"\udb76"
                               u"\ude3f"
                               u"\ud9e6"
                               u"\udac6"
                               u"\ude13"
                               u"\udba8"
                               u"\udc24"
                               u"\udc24"
                               u"\udfc5"
                               u"\udf84"
                               u"\udc74"
                               u"\udc74"
                               u"\u0000"
                               u"\x00"
                               u"\x0b"
                               u"\x0c"
                               u"\xe4"
                               u"\xbd"
                               u"\xa0"
                               u"\x01"
                               u"\xe5"
                               u"\xa5"
                               u"\xbd"
                               u"\xa0"
                               u"\xc3"
                               u"\xa4"
                               u"\xc2"
                               u"\x0f"
                               u"\x12"
                               u"\x15"
                               "]+", flags=re.UNICODE)
    return weridPatterns.sub(r'', text)
