import threading
import time
import logging
import random

import json
import requests
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
from fastavro import writer, reader, schema, parse_schema
from queue import Queue
from threading import Thread, Lock

weather_data_que = Queue(maxsize=-1)
region_list = ["Sundbyberg", "Södertälje", "Tyresö", "Täby", "Upplands Väsby", "Vallentuna", "Vaxholm", "Värmdö",
               "Österåker"]
count = len(region_list)
api_key = "MnnmAGnE9KHMVfG93KftKryEGoPhPAK1"

logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-9s) %(message)s', )


class CrawlWeatherDataThread(Thread):
    def __init__(self, region_list, group=None, target=None, name=None,
                 args=(), kwargs=None, verbose=None):

        super(CrawlWeatherDataThread, self).__init__()
        self.name = name
        self.region_list = region_list
        self.target = target
        self.url_lk = ("http://dataservice.accuweather.com/locations/v1/cities/search?apikey=%s&q=" % (api_key))
        return

    '''
    1. Get the location key of each region in Stockholm
    2. Get the weather data 
    3. Format the weather data
    4. Put the weather data in the weather data que
    '''

    def getLocationkey(self):
        location_key_que = Queue()
        while True:
            if len(self.region_list) <= 0:
                break
            region = self.region_list.pop()

            url_location_key = self.url_lk + region
            res = {}
            code = -200
            try:
                res = requests.get(url_location_key)

                code = res.status_code

            except Exception as e:
                print("ERROR", str(e))
                code = res.code
                continue

            data = res.json()
            if len(data) == 0:
                continue
            if len(data) != 0 and not data[0].get('Key'):
                print("WARNING, %s 's location key not found" % (region))
                continue
            else:
                location_key_que.put((region, data[0].get('Key')))
        return location_key_que

    def format_data(self, tables, region):
        newTables = []
        for i in range(len(tables)):
            if not tables:
                continue
            dateFields = tables[i]['LocalObservationDateTime'].split('T')
            yearFields, timeFields = dateFields[0].split('-'), dateFields[1]
            newTable = dict()
            newTable['Region'], newTable['Year'], newTable['Month'], newTable['Day'], newTable['Time'] = region, \
                                                                                                         yearFields[0], \
                                                                                                         yearFields[1], \
                                                                                                         yearFields[
                                                                                                             2], timeFields
            metric, imperial = {'Temperature_Metric': str(tables[i]['Temperature']['Metric'])}, {
                'Temperature_Imperial': str(tables[i]['Temperature']['Imperial'])}
            del_list = ['MobileLink', 'Link', 'LocalObservationDateTime', 'Temperature']
            for j in range(len(del_list)):
                tables[i].pop(del_list[j])
            newTable.update(tables[i])
            newTable.update(metric)
            newTable.update(imperial)
            newTables.append(newTable)
        return newTables

    def run(self):

        global count
        location_key_que = self.getLocationkey()
        count = location_key_que.qsize()

        while True:
            if location_key_que.empty():
                break
            region, location_key = location_key_que.get()
            url_wd = ("http://dataservice.accuweather.com/currentconditions/v1/%s/historical/24?apikey=%s" % (
            location_key, api_key))
            res = {}
            code = 200
            try:
                res = requests.get(url_wd)
            except  Exception  as e:
                print("ERROR", str(e))
                continue

            content = res.json()
            newTables = self.format_data(content, region)
            weather_data_que.put(newTables)
        print('thread %s opearation finished ' % self.name)
        return


class ParsetoAvroThread(Thread):
    def __init__(self, fp, lock, group=None, target=None, name=None,
                 args=(), kwargs=None, verbose=None):
        super(ParsetoAvroThread, self).__init__()
        self.target = target
        self.name = name
        self.lock = lock
        self.fp = fp
        return
        '''
        pop the data from waether data que, prase the data to avro and save it. 
        '''

    def run(self):
        global count
        print('Thread--%s--started successfully----' % self.name)
        while count > 0:
            if weather_data_que.empty():
                continue
            content = weather_data_que.get()
            self.parse_content(content)
            count -= 1
            logging.debug('Putting  items in queue')
        print('Thread-%s-opearation finished--' % self.name)

    def parse_content(self, content):

        weather_schema = {
            "name": "Weather_Data",
            "type": "record",
            "fields": [
                {'name': 'Region', 'type': 'string'},
                {'name': 'Year', 'type': 'string'},
                {'name': 'Month', 'type': 'string'},
                {'name': 'Day', 'type': 'string'},
                {'name': 'Time', 'type': 'string'},
                {'name': 'EpochTime', 'type': 'int'},
                {'name': 'WeatherIcon', 'type': 'int'},
                {'name': 'HasPrecipitation', 'type': 'boolean'},
                {'name': 'PrecipitationType', 'type': ['string', 'null']},
                {'name': 'IsDayTime', 'type': 'boolean'},
                {'name': 'Temperature_Metric', "type": 'string'},
                {'name': 'Temperature_Imperial', 'type': 'string'}

            ]
        }

        parsed_schema = parse_schema(weather_schema)
        self.lock.acquire()
        writer(self.fp, parsed_schema, content)
        self.lock.release()


if __name__ == '__main__':
    fp = open('test.avro', 'ab+')
    lock = Lock()

    p = CrawlWeatherDataThread(region_list, name='producer')
    c = ParsetoAvroThread(fp, lock, name='consumer')

    p.start()
    c.start()
