from confluent_kafka import Producer
import time, sys, random

class KafkaProducer():

    def __init__(self):
        self.p = Producer({'bootstrap.servers': 'localhost:9092'})

    #produce messages
    def produce_messages(self):
        self.a = 5
        while True:
            for data in range(int(sys.argv[2]),int(sys.argv[3])):
                self.p.poll(0)
                #produce messages with small numebers to demonstrate data completeness
                if self.a == 10000:
                    self.p.produce('test', str({'timestamp':time.time(),'sensor':sys.argv[1],'data':1,'cond':'yes'}).encode('utf-8'))
                    print("condition met")
                    self.a += 1

                # produce messages and sleep for 7 seconds to demonstrate lag and OOO
                if self.a == 20000:
                    self.p.produce('test', str({'timestamp':time.time(),'sensor':sys.argv[1],'data':data,'cond':'yes'}).encode('utf-8'))
                    print("condition met")
                    time.sleep(7)
                    self.a += 1
                    continue

                self.p.produce('test',str({'timestamp': time.time(),
                    'sensor': sys.argv[1], 'data': random.randint(int(sys.argv[2]),int(sys.argv[3]))}).encode('utf-8'))
                self.a += 1

            self.p.flush()


if __name__ == '__main__':
    kafka_obj = KafkaProducer()
    kafka_obj.produce_messages()
