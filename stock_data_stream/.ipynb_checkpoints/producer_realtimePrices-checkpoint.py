from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
import requests
import json
import time
import datetime
import logging
import threading
import src.data as dt


kafka_config = {
    'bootstrap.servers': 'localhost:9092'
}

# Create AdminClient
admin_client = AdminClient(kafka_config)
# Fetch metadata
metadata = admin_client.list_topics(timeout=10)
# Get and print all topics
topics = metadata.topics
# Kafka Producer Configuration
log_filename = f"logs/{datetime.datetime.now().strftime('%Y-%m-%d')}_producerRT_script.log"
logging.basicConfig(filename=log_filename, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', force=True)

icb_symbol = json.loads(open('assets/icb_symbol.json', 'r').read())
topic_names = list(icb_symbol.keys())

for t in topic_names:
    if t in list(topics.keys()):
        continue
    else:
        print(t, "is not in topics")

today = datetime.datetime.today().date().strftime("%Y-%m-%d")
start_time = datetime.time(8, 50)
end_time = datetime.time(15, 10)

if (datetime.datetime.now().time() > start_time and datetime.datetime.now().time() < end_time): 
    logging.info("Time ok")
    
def kafka_producer_thread(producer_id, topic_name):
    producer = Producer(kafka_config)
    tickers = icb_symbol[topic_name]
    loader = dt.DataLoader_json(symbols=tickers,
           start=today,
           end=today,
           minimal=False,
           data_source="cafe",
           table_style="prefix")
    print("LoDER DONE")
    while (datetime.datetime.now().time() > start_time and datetime.datetime.now().time() < end_time):
        data = loader.download()
        logging.info("Download done at %s", datetime.datetime.now().time())
        for k, v in data.items():	
            producer.produce(topic_name, value=json.dumps(v))
        producer.flush()
        time.sleep(60)

threads = []
for i, topic_name in enumerate(topic_names):
    thread = threading.Thread(target=kafka_producer_thread, args=(i, topic_name))
    threads.append(thread)
    thread.start()

# Join threads to the main thread
for thread in threads:
    thread.join()

