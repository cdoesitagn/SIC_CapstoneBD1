# Import thư viện
from confluent_kafka import Consumer, KafkaError
import pymongo
from pymongo import MongoClient
from bson import json_util
import json
import datetime
import logging
import threading
import time

# Tạo file log cho consumer endofday
log_filename = f"logs/{datetime.datetime.now().strftime('%Y-%m-%d')}_consumerEOD_script.log"
logging.basicConfig(filename=log_filename, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', force=True)

# Kết nối với stockDB 
c = MongoClient("100.89.103.30:27017", 27017)
db = c["stockDB"]
eodPrices = db.eodPrices
industriesDb = db.industries
tickersDb = db.tickers


# Kết nối với kafka
kafkaConfigs = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-consumer-group',
    'auto.offset.reset': 'earliest'
}
# Chèn các id của ticker và industry cho các mã chứng khoán tương ứng
def insertId(data):
    for dt in data:
        ticker = tickersDb.find_one({"symbol": dt["code"]})
        if ticker:
            dt['tickerId'] = ticker['_id']
            dt['industryId'] = ticker['industryId']
        else:
            print(f"No ticker found with symbol {dt['code']}")

# Đợi 60s để topic nhận được message mới nhất từ producer oed
time.sleep(60)
consumer = Consumer(kafkaConfigs)
# Đăng ký consumer với topic EOD
consumer.subscribe(["EOD"])


try: 
    while True:
        # Consumer lấy data từ topic EOD
        message = consumer.poll(1.0)  
        # Kiểm tra có lỗi đối với dữ liệu hay không, nếu không chuyển đổi dữ liệu, chèn Id vào các mã sau đó truyền vào collection eodPrices trong cơ sở dữ liệu
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
            eodPrices.insert_many(data)
            logging.info("Insert data done at %s ", datetime.datetime.now().time())
except Exception as e:
    print(f'Error: {e}')
finally:
    # Đóng lại consumer khi kết thúc
    consumer.close()
