# Import thư viện
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import requests
import json
import time
import datetime
import logging
import threading
import src.data as dt

# Kết nối với kafka
kafka_config = {
    'bootstrap.servers': 'localhost:9092'
}
admin_client = AdminClient(kafka_config)
producer = Producer(kafka_config)

# Tạo file log cho producer end of day
log_filename = f"logs/{datetime.datetime.now().strftime('%Y-%m-%d')}_producerEOD_script.log"
logging.basicConfig(filename=log_filename, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', force=True)

# Lấy danh sách tất cả các mã chứng khoán
icb_symbol = json.loads(open('assets/icb_symbol.json', 'r').read())
topic_names = list(icb_symbol.values())
tickers = []
for industry in topic_names:
    for i in industry:
        tickers.append(i)
print(tickers)

# Lấy thời gian ngày hôm nay
today = datetime.datetime.today().date().strftime("%Y-%m-%d")

# Tạo loader cho các mã chứng khoán hôm nay
loader = dt.DataLoader_json(symbols=tickers,
       start=today,
       end=today,
       minimal=False,
       data_source="cafe",
       table_style="prefix")

# Tải về dữ liệu các mã chứng khoán hôm nay
data = loader.download()
logging.info("Download done at %s", datetime.datetime.now().time())
for k, v in data.items():	
    # Đẩy data vào topic EOD trong cụm kafka
    producer.produce("EOD", value=json.dumps(v))
producer.flush()
