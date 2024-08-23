from confluent_kafka import Consumer, KafkaError
import pymongo
from pymongo import MongoClient
from bson import json_util
import json
import datetime
import logging
import threading
import time

log_filename = f"logs/{datetime.datetime.now().strftime('%Y-%m-%d')}_consumerEOD_script.log"
logging.basicConfig(filename=log_filename, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', force=True)

icb_symbol = json.loads(open('assets/icb_symbol.json', 'r').read())
topic_names = list(icb_symbol.keys())

c = MongoClient("100.89.103.30:27017", 27017)
db = c["stockDB"]
eodPrices = db.eodPrices
industriesDb = db.industries
tickersDb = db.tickers


# Kafka Consumer Configuration
kafkaConfigs = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-consumer-group',
    'auto.offset.reset': 'earliest'
}
def insertId(data):
    for dt in data:
        ticker = tickersDb.find_one({"symbol": dt["code"]})
        if ticker:
            dt['tickerId'] = ticker['_id']
            dt['industryId'] = ticker['industryId']
        else:
            print(f"No ticker found with symbol {dt['code']}")

time.sleep(60)
consumer = Consumer(kafkaConfigs)
consumer.subscribe(["EOD"])
try: 
    while True:
        message = consumer.poll(1.0)  # Adjust the timeout as needed
        if message is None:
            print("Message is None")
        if message.error():
            if message.error().code() == KafkaError._PARTITION_EOF:
                logging.info(f"Reached end of partition: {message.topic()} [{message.partition()}]")
            else:
                logging.info(f"Error while consuming from topic {message.topic()}: {message.error()}")
        else:
            data = json.loads(message.value().decode('utf-8'))
            insertId(data)
            
            # print(f"Received message at", datetime.datetime.now().time())
            eodPrices.insert_many(data)
            logging.info("Insert data done at %s ", datetime.datetime.now().time())
except Exception as e:
    print(f'Error: {e}')
finally:
    consumer.close()
