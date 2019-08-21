from elasticsearch import Elasticsearch
from pymongo import errors
from uuid import uuid4
from warcio.archiveiterator import ArchiveIterator
import elasticsearch
import hashlib
import json
import logging
import os
import pymongo
import re
import requests
import sys
import threading
import time


# Mongodb metadata collection cursor
mongodb = None

# elastic connector
es = None


def _logger_level_setup():
    """logger level setup
    """
    root = logging.getLogger()
    root.setLevel(logging.ERROR)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    root.addHandler(handler)


def _mongodb_connection_setup(dbName):
    """database setup
    """
    try:
        global mongodb
        client = pymongo.MongoClient('mongodb://mongodb-service:27017')
        mongodb = client[dbName]
        logging.debug('Mongodb connected')
    except errors.ConnectionFailure as error:
        logging.error(error, exc_info=True)


def _elastic_connection_setup(indexs):
    """elasticsearch setup
    """
    global es
    connected = False
    try:
        es = Elasticsearch(
            hosts=[{'host': 'elasticsearch-service'}], max_retries=100)
        es.ping()

    except elasticsearch.ConnectionError as error:
        logging.error(error, exc_info=True)
    except elasticsearch.ConnectionTimeout as error:
        logging.error(error, exc_info=True)
    # this line of code handles the sync bugs of elasticsearch in docker
    while not connected:
        try:
            es.cluster.health(wait_for_status="yellow")
            connected = True
            if connected:
                for index in indexs:
                    es.indices.create(
                        index=index['index'], ignore=400, body=index['mapping'])
                logging.debug('Elasticsearch connected')
        except ConnectionError as error:
            time.sleep(0.1)
            logging.error(error, exc_info=True)


def _collection_count():
    """this function is used for skip already fetched records

    Returns:
        {int} -- return the count of records, if the collection was cleaned up
        return 0
    """
    try:
        return mongodb.metadata.count()
    except errors.CursorNotFound as error:
        return 0


def _convert_tolower(headers):
    return {k.lower(): v for k, v in headers.items()}


def _insert_metadata(id, status, response_headers, warc_headers):
    """convert list of tuples of response header infomation
    to json object then store it into mongodb

    Arguments:
        status {int} -- the status code of response
        response_headers {[tuple]} -- a list of key-value response metadata
        warc_headers {[tuple]} -- a list of key-value warc metadata
    """
    # in case there is no response header
    if len(response_headers) and len(warc_headers) != 0:
        try:
            # convert all key to lower case to avoid sample field with different
            # string cases
            response_headers = dict(response_headers)
            warc_headers = dict(warc_headers)
            warc_headers['status'] = status
            response_headers = _convert_tolower(response_headers)
            warc_headers = _convert_tolower(warc_headers)
            headers = {'id': id, 'metadata': {'response':
                                              response_headers,
                                              'warc': warc_headers}}
            mongodb.metadata.insert_one(headers)
        except errors.OperationFailure as error:
            logging.error(error, exc_info=True)
        except errors.ExecutionTimeout as error:
            logging.error(error, exc_info=True)


def _insert_body_content(index, id, stream):
    """save the body raw content of warc file

    Arguments:
        stream {content_stream} -- the decompressed stream
    """
    try:
        body = stream.read()
        # find the encoding type
        has_encoding = re.findall(b"""<meta.*?charset=(.*?)[>\s]""", body)
        if has_encoding:  # when find encoding, use that encoding
            body = body.decode(
                has_encoding[0].decode('utf-8')
                .lower().strip(';:\' "\/'), 'backslashreplace')
        else:  # otherwise only use utf-8
            body = body.decode('utf-8', 'backslashreplace')
        serialized_body = json.dumps({'id': id, 'content': body})
        res = es.index(index=index+"_content", body=serialized_body)

    except errors.OperationFailure as error:
        logging.error(error, exc_info=True)
    except errors.ExecutionTimeout as error:
        logging.error(error, exc_info=True)


def _dump_metadata_to_elastic(index):
    try:
        metadatas = mongodb.metadata.find({})
        for metadata in metadatas:
            # since _id is not serilizable, it has to be removed
            _ = metadata.pop('_id')
            res = es.index(index=index+"_metadata", body=json.dumps(metadata))
    except elasticsearch.SerializationError as error:
        logging.error(error, exc_info=True)


def _fetch_records(index, url, maxRecords=5):
    """fetch the warc record by given url and maxRecords, when maxRecords
    reaches the fetching loop will be broken

    Arguments:
        url {string} -- warc file url 

    Keyword Arguments:
        maxRecords {int} -- [the maximum records to fetch] (default: {5})
    """

    # {int} the number to skip fetch
    stored_records = _collection_count()

    resp = requests.get(url, stream=True)
    for record in ArchiveIterator(resp.raw, arc2warc=True):
        if record.rec_type == 'response':
            if record.http_headers.get_header('Content-Type') == 'text/html':
                if stored_records > 0:
                    stored_records -= 1
                    continue
                # {string} the id for both metadata and content body
                id = uuid4().hex
                _insert_metadata(id, record.http_headers.get_statuscode(),
                                 record.http_headers.headers,
                                 record.rec_headers.headers)
                _insert_body_content(index, id, record.content_stream())
                maxRecords -= 1
                if maxRecords == 0:
                    break


def _clean_up_database():
    """clearn up mongdb collection
    """
    try:
        mongodb.metadata.drop()
        logging.debug('mongodb cleaned up')
    except errors.InvalidOperation as error:
        logging.error(error, exc_info=True)
    except errors.OperationFailure as error:
        logging.error(error, exc_info=True)


def _clean_up_elastic(index='test'):
    """clean up index for further testing

    Keyword Arguments:
        index {str} -- elasticsearch index (default: {'test'})
    """
    try:
        es.indices.delete(index=index+"_content", ignore=[
                          400, 404])
        es.indices.delete(index=index+"_metadata", ignore=[
                          400, 404])
        logging.debug('elasticsearch index cleaned up')
    except elasticsearch.NotFoundError as error:
        logging.error(error, exc_info=True)
    except elasticsearch.RequestError as error:
        logging.error(error, exc_info=True)


def _clean_up(index):
    """the combination of clean up methods
    """
    _clean_up_database()
    _clean_up_elastic(index)


def query_elastic(index='test'):
    """sample elastic query

    Keyword Arguments:
        index {str} -- which index you want to search (default: {'test'})
    """
    try:
        res = es.search(index=index,
                        body={'query': {'match_all': {}}})
        logging.info(res)
    except elasticsearch.NotFoundError as error:
        logging.error(error, exc_info=True)


def setup_all_connections(dbName):
    _logger_level_setup()
    _mongodb_connection_setup(dbName)
    indexs = [
        {'index': dbName+'_content',
         'mapping': '''
            {
                "mappings":{
                    "_doc":{
                        "properties":{
                            "id": {"type": "text"},
                            "content": {"type": "text"}
                        }
                    }
                }
            }
         '''
         },
        {'index': dbName+'_metadata',
         'mapping': '''
            {
                "mappings":{
                    "_doc":{
                        "properties":{
                            "id": {"type": "text"},
                            "metadata": {
                                "dynamic": true,
                                }
                        }
                    }
                }
            }
         '''
         }
    ]
    _elastic_connection_setup(indexs)


def fetch_store_or_cleanup(dbName, url, maxRecordsFetch):
    """the main action of this project, there are three actions
    to take based on maxRecordsFetch.
    - 0: doing nothing
    - >0: fetch the given number of records
    - <0: cleanup database and elasticsearch

    Arguments:
        maxRecordsFetch {int} -- the max number of records to fetch each time
    """
    if maxRecordsFetch > 0:
        logging.debug('Metadata collection task is running...')
        _fetch_records(dbName, url, maxRecordsFetch)
        _dump_metadata_to_elastic(dbName)
    # if maxRecordsFetch is negative, actioin of clean up
    # database and elasticsearch will be taken
    elif maxRecordsFetch < 0:
        _clean_up(dbName)
    else:
        return


def task_runner():
    url = "https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2018-26/segments/1529267859766.6/warc/CC-MAIN-20180618105733-20180618125538-00027.warc.gz"
    while True:
        try:
            # the envionment variable that passed from docker-compose file
            maxRecordFetch = int(os.getenv('MAX_RECORD_FETCH', 100))
            # dbName can be used both on mongodb database name and
            # elasticsearch index name
            dbName = os.getenv('DB_NAME', 'test')

            # setup mongodb and elastic context environment
            setup_all_connections(dbName)

            # main task part, fetch warc files and store them into database
            # and indexing in elastich search, or for experiment purpose
            # these can be cleaned up
            fetch_store_or_cleanup(dbName, url, maxRecordFetch)

        except ValueError as e:
            logging.error(e, exc_info=True)
        except Exception as e:
            logging.debug('Exception did not handled')
            logging.error(e, exc_info=True)
        finally:
            return


if __name__ == '__main__':
    thread = threading.Thread(target=task_runner)
    thread.start()
    thread.join()
