import json
import logging
import time

import elasticsearch
from elasticsearch import Elasticsearch

from models import Failure
from prometheus import count_reindex_kavosh, count_reindex_map, count_reindex_rtp, error_count_reindex_kavosh, \
    error_count_reindex_map, error_count_reindex_rtp
from settings import config
import datetime
from db import Session, engine, Base

Base.metadata.create_all(engine)

es = Elasticsearch([config['ELASTIC_HOST_REINDEX']], timeout=int(config['TIMEOUT']))
es1 = Elasticsearch([config['ELASTIC_HOST_REQUEST_TIME']],
                    timeout=int(config['TIMEOUT']))  # request for  get the first time



class Reindex:
    def __init__(self, index_name, file_name):
        self.file_name = file_name
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

        logging.debug('Reindex:get_start_time before request is gte_time :' + str(gte_time))
        logging.debug('Reindex:get_start_time before request is info_time:' + str(info_time))
        if len(info_time) == 0 or gte_time != 0:
            if self.index_name == config['MAP_INDEX']:
                sort = [{"indexed_time": {"order": "asc"}}]
                range = {"range": {"indexed_time": {"gte": int(gte_time), "boost": 2.0}}}
            else:
                sort = [{"info.index_time": {"order": "asc"}}]
                range = {"range": {"info.index_time": {"gte": int(gte_time), "boost": 2.0}}}
            try:
                query = {"size": 1, "sort": sort,
                         "query": range}
                res = es1.search(query, index=self.index_name_star)
                res = res['hits']['hits']

                if len(res):
                    if self.index_name == config['MAP_INDEX']:
                        info_time = res[0]['_source']['indexed_time']
                    else:
                        info_time = res[0]['_source']['info']['index_time']
                    logging.debug("Reindex:get_start_time after request info_time: " + str(info_time))
                else:
                    logging.debug("Reindex:get_start_time not exists index")

                    return "NOTEXIST"
            except elasticsearch.ElasticsearchException as e:
                logging.error("Rindex:get_start_time  error exception:" + str(e))

                return "NOTEXIST"
        logging.debug("Reindex:get_start_time after request return info_time: " + str(info_time))
        return info_time

    def query(self, start_time, end_time):
        time_dest = int(int(start_time) / 1000)
        dest_index = 'dd' + self.index_name + (datetime.datetime.fromtimestamp(time_dest).strftime('%Y-%m-%d'))
        logging.debug("REINDEX:query  dest_index : " + dest_index)
        if self.index_name == config['MAP_INDEX']:
            range = {"range": {"indexed_time": {"gte": int(start_time), "lte": end_time, "boost": 2.0}}}
        else:
            range = {"range": {"info.index_time": {"gte": int(start_time), "lte": end_time, "boost": 2.0}}}
        query = {

            "source": {
                "index": self.index_name_star,
                "size": 100,
                "remote": {
                    "host": config['REMOTE_HOST_SOURCE']
                },
                "query": range
            },
            "dest": {
                "index": dest_index
            }
        }
        logging.info("Reindex:query :" + json.dumps(query))
        return query

    def checktime(self, start_time, end_time):
        try:
            start = datetime.datetime.fromtimestamp(int(int(start_time) / 1000))
            end = datetime.datetime.fromtimestamp(int(int(end_time) / 1000))
            if start.day == end.day:
                return start_time, end_time
            else:
                end = int(datetime.datetime(year=start.year, month=start.month, day=start.day, hour=23, minute=59,
                                            second=59).timestamp()) * 1000
                return start_time, end
        except Exception as e:
            logging.error("Reindex:checktime " + str(e))
            return 0, 0

    def reindex(self):
        try:
            start_time = self.get_start_time()
            time_now = int(time.time()) * 1000
            if int(start_time) < int(time_now):
                if start_time != "NOTEXIST":
                    logging.debug("start_time " + str(start_time))
                    end_time = int(start_time) + (int(config['ELASTIC_DURATION']) * 1000)
                    start_time, end_time = self.checktime(start_time, end_time)
                    query = self.query(start_time, end_time)
                    try:
                        res = es.reindex(query)
                        logging.info("reindex result " + json.dumps(res))
                        if res['updated'] or res['created']:
                            if self.index_name == config['KAVOSH_INDEX']:
                                count_reindex_kavosh(res['created'])
                            elif self.index_name == config['MAP_INDEX']:
                                count_reindex_map(res['created'])
                            elif self.index_name == config['RTP_INDEX']:
                                count_reindex_rtp(res['created'])

                            logging.debug("Reindex:reindex elastic reindex  created")
                            self.write_file(end_time)
                        else:
                            gte_time = self.read_file()
                            logging.debug('Reindex:reindex  res[updated] and res[created] null')
                            if len(gte_time) == 0:
                                gte_time = 0
                            s_time = self.get_start_time(gte_time)
                            self.write_file(s_time)

                    except elasticsearch.ElasticsearchException as e:
                        if e.status_code != 'N/A':
                            logging.debug("add error end time is :" + str(end_time) + "  index_name " + self.index_name)
                            self.write_file(end_time)
                            session = Session()
                            failure = Failure(start_time=start_time, end_time=end_time, index_name=self.index_name_star)
                            session.add(failure)
                            session.commit()
                            Session.remove()
                        else:
                            if self.index_name == config['KAVOSH_INDEX']:
                                error_count_reindex_kavosh(res['created'])
                            elif self.index_name == config['MAP_INDEX']:
                                error_count_reindex_map(res['created'])
                            elif self.index_name == config['RTP_INDEX']:
                                error_count_reindex_rtp(res['created'])
                        logging.error("Reindex:reindex error elastic reindex " + str(e))
                    logging.debug("Reindex:reindex end_time write file " + str(end_time))

        except Exception as e:
            logging.warning("Reindex:reindex  " + str(e))
        time.sleep(int(config['TIME_SLEEP']))
        self.reindex()
        return 'ok'

    def check_failure(self):
        try:
            session = Session()

            failurs = session.query(Failure).all()

            for item in failurs:
                query = self.query(item.start_time, item.end_time)
                try:
                    res = es.reindex(query)
                    logging.info("check failoure " + json.dumps(res))
                    session.query(Failure).filter(Failure.id == item.id).delete()
                    session.commit()
                    if self.index_name == config['KAVOSH_INDEX']:
                        count_reindex_kavosh(res['created'])
                    elif self.index_name == config['MAP_INDEX']:
                        count_reindex_map(res['created'])
                    elif self.index_name == config['RTP_INDEX']:
                        count_reindex_rtp(res['created'])
                except elasticsearch.ElasticsearchException as e:
                    logging.error("proplem index failure " + str(e))
                    if self.index_name == config['KAVOSH_INDEX']:
                        error_count_reindex_kavosh(res['created'])
                    elif self.index_name == config['MAP_INDEX']:
                        error_count_reindex_map(res['created'])
                    elif self.index_name == config['RTP_INDEX']:
                        error_count_reindex_rtp(res['created'])

            Session.remove()

        except Exception as e:
            logging.error("Reindex:check_failure " + str(e))
        time.sleep(int(config['TIME_SLEEP']))
        self.check_failure()
        return "ok"

    def read_file(self):
        file = open(self.file_name, 'r')
        time = file.read()
        file.close()
        return time

    def write_file(self, end_time):
        try:
            if (end_time != "NOTEXIST"):
                file = open(self.file_name, 'w')
                file.write(str(end_time))
                file.close()

        except Exception as e:
            logging.error("write_file error " + str(e))
