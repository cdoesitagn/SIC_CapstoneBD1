# Import thư viện
import pymongo
from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
from scipy.signal import savgol_filter

# Kết nối với cơ sở dữ liệu để lấy thông tin về giá
c = MongoClient("100.89.103.30:27017", 27017)
db = c["stockDB"]
industry = db.industries
eodPrices = db.eodPrices

# Tìm ra tất cả thông tin về giá cảu ngành ngân hàng
nganhangID = industry.find_one({"name": "Ngân hàng"}, {"_id": 1})["_id"]
nganhang_eodPrices = eodPrices.find({"industryId": nganhangID})
nganhang_df = pd.DataFrame(nganhang_eodPrices)
nganhang_df['date'] = pd.to_datetime(nganhang_df['date']).dt.strftime("%Y-%m-%d")
nganhang_df = nganhang_df[["date", "open", "high", "low", "close", "volume_match"]].groupby('date').mean().reset_index()
nganhang_df.rename(columns={'date': "Date"}, inplace=True)
# Đọc tập dữ liệu các bài báo đã kéo được từ trang CafeF https://cafef.vn/
articles_data = pd.read_csv("data/tai_chinh_ngan_hang.csv")[["Published_Date", "Title", "Text"]].dropna()
articles_data["Published_Date"] = pd.to_datetime(articles_data["Published_Date"]).dt.strftime("%Y-%m-%d")
articles_data.rename(columns={"Published_Date": "Date"}, inplace=True)
articles_data.sort_values(by = 'Date', inplace=True)
articles_data.reset_index(inplace = True)
articles_data = articles_data.drop("index", axis=1)

# Hợp nhất tất cả tiêu đề và nội dung bài báo của cùng một ngày
title_data = articles_data.groupby('Date')["Title"].apply(', '.join).reset_index()
# Gán các nhãn đặc trưng về giá Near, Mid, High với thông tin các bài báo theo ngày
labeled = pd.merge(title_data, nganhang_df[["Date", "open", "high", "low", "close", "volume_match"]], on='Date', how='left').dropna()
labeled.to_csv("data/labeled_data.csv")


