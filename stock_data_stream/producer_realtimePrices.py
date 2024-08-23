# Import thư viện
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
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
metadata = admin_client.list_topics(timeout=10)
# Lưu tất cả các topic
topics = metadata.topics

# Tạo file log cho producer realtime
log_filename = f"logs/{datetime.datetime.now().strftime('%Y-%m-%d')}_producerRT_script.log"
logging.basicConfig(filename=log_filename, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', force=True)

# Đọc các ngành đang có, mỗi ngành tương đương với mỗi topic 
icb_symbol = json.loads(open('assets/icb_symbol.json', 'r').read())
topic_names = list(icb_symbol.keys())

# Kiểm tra các topic đang có trong kafka tồn tại trong các ngành xem xét hay không
for t in topic_names:
    if t in list(topics.keys()):
        continue
    else:
        print(t, "is not in topics")

# Lấy thời gian ngày hôm nay, thời gian mở 8h15 sáng và thời gian đóng 15h10 chiều
today = datetime.datetime.today().date().strftime("%Y-%m-%d")
start_time = datetime.time(8, 50)
end_time = datetime.time(15, 10)

# Kiểm tra file có được bật trong khung giờ quy định hay không
if (datetime.datetime.now().time() > start_time and datetime.datetime.now().time() < end_time): 
    logging.info("Time ok")

# Khai báo thread, mỗi thread tương ứng với một producer
def kafka_producer_thread(producer_id, topic_name):
    producer = Producer(kafka_config)
    tickers = icb_symbol[topic_name]
    # Khai báo loader các dữ liệu của các ngành trong topic đó
    loader = dt.DataLoader_json(symbols=tickers,
           start=today,
           end=today,
           minimal=False,
           data_source="cafe",
           table_style="prefix")
    while (datetime.datetime.now().time() > start_time and datetime.datetime.now().time() < end_time):
        # Loader tải về các dữ liệu
        data = loader.download()
        logging.info("Download done at %s", datetime.datetime.now().time())
        for k, v in data.items():	
            # Đẩy dữ liệu vào cụm kafka theo topic tương ứng
            producer.produce(topic_name, value=json.dumps(v))
        producer.flush()
        # Truyền dữ liệu trong khoảng thời gian 1 phút / 60 s
        time.sleep(60)

# Tạo list các thread, tạo và kích hoạt các thread
threads = []
for i, topic_name in enumerate(topic_names):
    thread = threading.Thread(target=kafka_producer_thread, args=(i, topic_name))
    threads.append(thread)
    thread.start()

# Join threads to the main thread
for thread in threads:
    thread.join()

