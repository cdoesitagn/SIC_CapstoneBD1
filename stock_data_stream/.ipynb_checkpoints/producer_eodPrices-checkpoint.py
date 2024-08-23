from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import requests
import json
import time
import datetime
import logging
import threading
import src.data as dt

# Kafka Producer Configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9092'
}

# Create AdminClient
admin_client = AdminClient(kafka_config)
# Fetch metadata
metadata = admin_client.list_topics(timeout=10)
# Get and print all topics
topics = metadata.topics

# Delete the topic
# fs = admin_client.delete_topics(["EOD"], operation_timeout=30)
# for topic, future in fs.items():
#     try:
#         future.result()  # The result itself is None
#         print(f"Topic '{topic}' deleted successfully.")
#     except Exception as e:
#         print(f"Failed to delete topic '{topic}': {e}")
# time.sleep(10)
# # Recreate the topic
# new_topic = NewTopic("EOD", num_partitions=3, replication_factor=1)
# fs = admin_client.create_topics([new_topic], operation_timeout=30)
# for topic, future in fs.items():
#     try:
#         future.result()  # The result itself is None
#         print(f"Topic '{topic}' created successfully.")
#     except Exception as e:
#         print(f"Failed to create topic '{topic}': {e}")
        
log_filename = f"logs/{datetime.datetime.now().strftime('%Y-%m-%d')}_producerEOD_script.log"
logging.basicConfig(filename=log_filename, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', force=True)

icb_symbol = json.loads(open('assets/icb_symbol.json', 'r').read())
topic_names = list(icb_symbol.values())
tickers = []
for industry in topic_names:
    for i in industry:
        tickers.append(i)
print(tickers)

today = datetime.datetime.today().date().strftime("%Y-%m-%d")
# # print(today)
# # def kafka_producer_thread(producer_id, topic_name):
producer = Producer(kafka_config)
loader = dt.DataLoader_json(symbols=tickers,
       # start=today,
       start=today,
       end=today,
       # end="2024-08-15",
       minimal=False,
       data_source="cafe",
       table_style="prefix")
data = loader.download()
logging.info("Download done at %s", datetime.datetime.now().time())
print("Download done at %s", datetime.datetime.now().time())
for k, v in data.items():	
    producer.produce("EOD", value=json.dumps(v))
producer.flush()

# threads = []
# for i, topic_name in enumerate(topic_names):
#     thread = threading.Thread(target=kafka_producer_thread, args=(i, topic_name))
#     threads.append(thread)
#     thread.start()

# # Join threads to the main thread
# for thread in threads:
#     thread.join()