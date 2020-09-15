from confluent_kafka import Consumer
import time, math
import pandas as pd
from DictTTL.DefaultDictTTL import DefaultDictTTL
from ast import literal_eval

pd.set_option('display.float_format', lambda x: '%.3f' % x)

class KafkaProducer():

    def __init__(self):
        self.time_to_live = 5
        self.dict_ttl = DefaultDictTTL(time_to_live=self.time_to_live)
        self.df = pd.DataFrame()
        self.ttl = 0

    def ConsumeMessages(self):
        c = Consumer({
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'mygroup2',
            'auto.offset.reset': 'largest'
        })

        c.subscribe(['test'])
        self.counter = 0
        if self.counter == 0:
            self.now = math.floor(time.time() - (time.time() % 10) + 1)
            self.end = self.now + 5
            self.counter += 1

        while True:
            msg = c.poll(0.1)
            if msg is None:
                continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue

            self.data = literal_eval(msg.value().decode('utf-8'))

            if math.floor(self.data['timestamp']) < self.now:
                pass

            if (math.floor(self.data['timestamp']) >= self.now) and (math.floor(self.data['timestamp']) < self.end):
                self.dict_ttl.append_values(self.data['sensor'], self.data["data"])

            if math.floor(self.data['timestamp']) >= self.end:
                print('\nLength of dict is {}'.format(len(list(self.dict_ttl))))
                if len(list(self.dict_ttl)) == 0:
                    self.now = self.now + 5
                    self.end = self.end + 5
                    continue
                for k,v in self.dict_ttl.ttl_items():
                    self.df = self.df.append({'timestamp':self.now,
                                                        'data':v[0][1], 'sensor':k}, ignore_index=True)
                if self.df.empty:
                    print('empty df')
                    continue

                print('sum of items is {}'.format(str(self.df['data'].apply(lambda x:sum(x)).tolist()[0])))
                self.total = self.df['data'].apply(lambda x:sum(x)).tolist()[0]
                if int(self.total) < 7000:
                    print('This row would be discarded')
                self.df['data'] = self.df['data'].apply(lambda x:sum(x) / len(x))

                self.df = self.df.groupby(['timestamp','sensor']).agg('mean').reset_index()
                print("Mean of all items for this sensor within window {},{} is ".format(self.now, self.end))
                print(self.df)
                print('\n---------------------------')
                self.df = pd.DataFrame()
                self.dict_ttl = DefaultDictTTL(5)
                self.now = self.now + 5
                self.end = self.end + 5

        c.close()

if __name__ == '__main__':
    kafka_obj = KafkaProducer()
    kafka_obj.ConsumeMessages()