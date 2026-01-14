import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer

# ==========================================
# 1. CẤU HÌNH HỆ THỐNG
# ==========================================
# Cập nhật đúng port Kafka của bạn (ví dụ: 9097, 9098, 9099)
producer = KafkaProducer(
    bootstrap_servers=['localhost:9097', 'localhost:9098', 'localhost:9099'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = "example_topic"

# ==========================================
# 2. DANH MỤC DỮ LIỆU GIẢ LẬP VIỆT NAM
# ==========================================

CITIES = {
    "Hà Nội": {
        "districts": ["Hoàn Kiếm", "Ba Đình", "Cầu Giấy", "Đống Đa", "Hai Bà Trưng", "Thanh Xuân", "Long Biên"],
        "streets": ["Nguyễn Trãi", "Lê Duẩn", "Phố Huế", "Trần Hưng Đạo", "Xuân Thủy", "Giải Phóng", "Cầu Giấy"],
        "lat_range": (21.0, 21.1),
        "long_range": (105.8, 105.9)
    },
    "TP. Hồ Chí Minh": {
        "districts": ["Quận 1", "Quận 3", "Quận 7", "Quận Tân Bình", "Quận Bình Thạnh", "TP. Thủ Đức"],
        "streets": ["Lê Lợi", "Nguyễn Huệ", "Cách Mạng Tháng 8", "Nam Kỳ Khởi Nghĩa", "Phạm Văn Đồng", "Nguyễn Thị Minh Khai"],
        "lat_range": (10.7, 10.8),
        "long_range": (106.6, 106.7)
    },
    "Đà Nẵng": {
        "districts": ["Hải Châu", "Sơn Trà", "Ngũ Hành Sơn", "Liên Chiểu", "Thanh Khê"],
        "streets": ["Võ Nguyên Giáp", "Nguyễn Văn Linh", "Trần Hưng Đạo", "Lê Duẩn", "Bạch Đằng"],
        "lat_range": (16.0, 16.1),
        "long_range": (108.2, 108.3)
    }
}

BROKERS = ["CenLand", "Đất Xanh Group", "Savills Việt Nam", "CBRE", "Vinhomes", "Môi giới tự do"]
HOME_TYPES = ["SINGLE_FAMILY", "CONDO", "MULTI_FAMILY", "APARTMENT"]
NLP_KEYWORDS = ["sổ đỏ chính chủ", "nở hậu", "mặt phố", "ô tô vào nhà", "full nội thất", "view hồ", "kinh doanh tốt"]

# ==========================================
# 3. HÀM SINH DỮ LIỆU
# ==========================================

def gen_timestamp():
    return int(datetime.now().timestamp() * 1000)

def generate_vietnam_data():
    city_name = random.choice(list(CITIES.keys()))
    city_info = CITIES[city_name]
    
    district = random.choice(city_info["districts"])
    street = random.choice(city_info["streets"])
    house_num = random.randint(1, 500)
    address = f"{house_num} Phố {street}, {district}, {city_name}"
    
    # Giá từ 2 tỷ đến 50 tỷ VNĐ
    price = random.randint(2_000_000_000, 50_000_000_000)
    
    # Diện tích từ 30m2 đến 300m2
    area = random.randint(30, 300)
    bedrooms = random.randint(1, 6)
    bathrooms = random.randint(1, bedrooms + 1)
    
    # NLP Description
    keyword = random.choice(NLP_KEYWORDS)
    description = f"Bán nhà {city_name}, {district}. {address}. {keyword}, giá rẻ, hỗ trợ vay ngân hàng."
    
    # Tạo Schema hoàn chỉnh khớp với Spark/Cassandra
    record = {
        "timestamp": gen_timestamp(),
        "zpid": str(random.randint(10000000, 99999999)),
        "homeStatus": "FOR_SALE",
        "detailUrl": f"https://gia-lap-nha-dat.vn/p/{random.randint(1, 99999)}",
        "address": address,
        "streetAddress": f"{house_num} {street}",
        "city": city_name,
        "state": "VN",
        "country": "Vietnam",
        "zipcode": "100000",
        "latitude": random.uniform(city_info["lat_range"][0], city_info["lat_range"][1]),
        "longitude": random.uniform(city_info["long_range"][0], city_info["long_range"][1]),
        "homeType": random.choice(HOME_TYPES),
        "price": float(price),
        "currency": "VND",
        "zestimate": int(price * random.uniform(0.9, 1.1)),
        "rentZestimate": int(price * 0.003),
        "taxAssessedValue": int(price * 0.7),
        "lotAreaValue": float(area),
        "lotAreaUnit": "m2",
        "bathrooms": bathrooms,
        "bedrooms": bedrooms,
        "livingArea": int(area),
        "daysOnZillow": random.randint(0, 30),
        "isFeatured": random.choice([True, False]),
        "isPreforeclosureAuction": False,
        "timeOnZillow": 0,
        "isNonOwnerOccupied": True,
        "isPremierBuilder": False,
        "isZillowOwned": False,
        "isShowcaseListing": random.choice([True, False]),
        "imgSrc": "https://file4.batdongsan.com.vn/images/no-image.png",
        "hasImage": True,
        "brokerName": random.choice(BROKERS),
        "description": description,
        "listingSubType": {
            "is_FSBA": True,
            "is_openHouse": False,
            "is_newHome": random.choice([True, False])
        },
        "priceChange": 0,
        "datePriceChanged": gen_timestamp(),
        "openHouse": None,
        "priceReduction": None,
        "unit": None,
        "newConstructionType": None,
        "videoCount": 0
    }
    return record

# ==========================================
# 4. VÒNG LẶP GỬI DỮ LIỆU (30S - 1P)
# ==========================================

def send_data():
    print(f"🚀 Producer đã khởi động. Đang gửi dữ liệu giả lập vào topic: {TOPIC_NAME}")
    print("Tần suất: 30 - 60 giây / 1 bản ghi.")
    try:
        while True:
            data = generate_vietnam_data()
            producer.send(TOPIC_NAME, value=data)
            
            # In ra màn hình để theo dõi
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Đã gửi: {data['address']} | {data['price']:,.0f} VND")
            
            # Thời gian nghỉ ngẫu nhiên từ 30 đến 60 giây
            sleep_time = random.randint(5,10)
            print(f"--- Nghỉ {sleep_time} giây trước bản ghi tiếp theo ---\n")
            time.sleep(sleep_time)
            
    except KeyboardInterrupt:
        producer.close()
        print("\n🛑 Producer đã dừng.")

if __name__ == "__main__":
    send_data()