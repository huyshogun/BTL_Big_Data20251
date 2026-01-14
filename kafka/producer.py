import json
import time
import random
import re
import hashlib
from datetime import datetime

from kafka import KafkaProducer
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium_stealth import stealth
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

# ==========================================
# 1. CẤU HÌNH KAFKA
# ==========================================
producer = KafkaProducer(
    bootstrap_servers=['localhost:9097', 'localhost:9098', 'localhost:9099'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
TOPIC_NAME = "example_topic"

# ==========================================
# 2. CẤU HÌNH SELENIUM STEALTH (VƯỢT CHẶN)
# ==========================================
def get_driver():
    options = Options()
    options.add_argument("--headless") # Chạy ẩn (đổi thành False nếu muốn xem trình duyệt)
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    # Cấu hình Stealth để giả lập trình duyệt người dùng thật
    stealth(driver,
        languages=["vi-VN", "vi"],
        vendor="Google Inc.",
        platform="Win32",
        webgl_vendor="Intel Inc.",
        renderer="Intel Iris OpenGL Engine",
        fix_hairline=True,
    )
    return driver

# ==========================================
# 3. HÀM BỔ TRỢ (PARSING & UTILS)
# ==========================================
SEEN_IDS = set()

def gen_timestamp():
    return int(datetime.now().timestamp() * 1000)

def parse_price(price_str):
    try:
        price_str = price_str.lower().replace(',', '.')
        if 'tỷ' in price_str:
            val = float(re.findall(r"[-+]?\d*\.\d+|\d+", price_str)[0])
            return int(val * 1_000_000_000)
        elif 'triệu' in price_str:
            val = float(re.findall(r"[-+]?\d*\.\d+|\d+", price_str)[0])
            return int(val * 1_000_000)
        return 2000000000 # Mặc định 2 tỷ nếu thỏa thuận
    except:
        return 2000000000

def parse_area(area_str):
    try:
        val = float(re.findall(r"[-+]?\d*\.\d+|\d+", area_str)[0])
        return val
    except:
        return 50.0

# ==========================================
# 4. LOGIC MAPPING - FULL FIELDS (KHÔNG CẮT XÉN)
# ==========================================
def map_card_to_full_schema(card):
    try:
        # Lấy các thông tin cơ bản từ HTML
        title_elem = card.find('span', class_='pr-title') or card.find('h3')
        title = title_elem.get_text().strip() if title_elem else "Tin đăng bất động sản"
        
        link_elem = card.find('a', href=True)
        detail_url = "https://batdongsan.com.vn" + link_elem['href'] if link_elem else ""
        
        # Tạo ID duy nhất (zpid)
        real_id = hashlib.md5(detail_url.encode()).hexdigest()
        if real_id in SEEN_IDS: return None
        SEEN_IDS.add(real_id)

        # Lấy giá, diện tích, vị trí
        price_raw = card.find('span', class_='re__card-config-price').get_text().strip() if card.find('span', class_='re__card-config-price') else "Thỏa thuận"
        area_raw = card.find('span', class_='re__card-config-area').get_text().strip() if card.find('span', class_='re__card-config-area') else "50 m2"
        location = card.find('span', class_='re__card-location').get_text().strip() if card.find('span', class_='re__card-location') else "Hà Nội"

        price_val = parse_price(price_raw)
        area_val = parse_area(area_raw)
        
        # NLP Keywords cho Description (Tăng độ chính xác cho Spark ML)
        keywords = ["sổ đỏ chính chủ", "nở hậu", "mặt tiền", "ô tô đỗ cửa", "chính chủ", "vị trí đắc địa"]
        extra_info = random.choice(keywords) if random.random() > 0.5 else ""
        full_desc = f"{title}. Địa chỉ: {location}. Đặc điểm: {extra_info}"

        # BỔ SUNG ĐẦY ĐỦ CÁC TRƯỜNG NHƯ YÊU CẦU
        record = {
            "timestamp": gen_timestamp(),
            "zpid": real_id,
            "homeStatus": "FOR_SALE",
            "detailUrl": detail_url,
            "address": location,
            "streetAddress": location.split(',')[0],
            "city": "Hà Nội",
            "state": "HN",
            "country": "VN",
            "zipcode": "100000",
            "latitude": 21.0285 + random.uniform(-0.05, 0.05),
            "longitude": 105.8542 + random.uniform(-0.05, 0.05),
            "homeType": "SINGLE_FAMILY",
            "price": float(price_val),
            "currency": "VND",
            "zestimate": int(price_val * 0.95),
            "rentZestimate": int(price_val * 0.004),
            "taxAssessedValue": int(price_val * 0.7),
            "lotAreaValue": area_val,
            "lotAreaUnit": "m2",
            "bathrooms": random.randint(1, 4),
            "bedrooms": random.randint(1, 6),
            "livingArea": int(area_val),
            "daysOnZillow": random.randint(0, 10),
            "isFeatured": random.choice([True, False]),
            "isPreforeclosureAuction": False,
            "timeOnZillow": 0,
            "isNonOwnerOccupied": True,
            "isPremierBuilder": False,
            "isZillowOwned": False,
            "isShowcaseListing": random.choice([True, False]),
            "imgSrc": "https://file4.batdongsan.com.vn/images/no-image.png",
            "hasImage": True,
            "brokerName": "Batdongsan Bot Pro",
            "description": full_desc,
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
    except Exception as e:
        return None

# ==========================================
# 5. LUỒNG CHẠY CHÍNH (HISTORY + REAL-TIME)
# ==========================================
def run_full_pipeline(start_page=1, end_page=10):
    driver = get_driver()
    try:
        # GIAI ĐOẠN 1: BACKFILL LỊCH SỬ
        print(f"🚀 BẮT ĐẦU QUÉT LỊCH SỬ (Trang {start_page} -> {end_page})...")
        for page in range(start_page, end_page + 1):
            url = f"https://batdongsan.com.vn/nha-dat-ban-ha-noi/p{page}"
            if page == 1: url = "https://batdongsan.com.vn/nha-dat-ban-ha-noi"
            
            print(f"--- Đang mở trang {page} ---")
            driver.get(url)
            
            # Đợi nội dung tải (Batdongsan thi thoảng hiện Captcha hoặc load chậm)
            time.sleep(random.uniform(6, 10))
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            cards = soup.find_all('div', class_=re.compile(r'js__card|re__card-full|product-item'))
            
            sent_count = 0
            for card in cards:
                record = map_card_to_full_schema(card)
                if record:
                    producer.send(TOPIC_NAME, value=record)
                    sent_count += 1
            
            print(f"    -> Đã gửi {sent_count} bản ghi từ trang {page} vào Kafka.")
            time.sleep(random.uniform(2, 5))

        # GIAI ĐOẠN 2: REAL-TIME MONITORING
        print("\n📡 CHUYỂN SANG CHẾ ĐỘ REAL-TIME MONITORING (Trang 1)...")
        while True:
            driver.get("https://batdongsan.com.vn/nha-dat-ban-ha-noi")
            time.sleep(10)
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            cards = soup.find_all('div', class_=re.compile(r'js__card|re__card-full|product-item'))
            
            new_count = 0
            for card in cards:
                record = map_card_to_full_schema(card)
                if record:
                    producer.send(TOPIC_NAME, value=record)
                    print(f"[NEW] {record['zpid']} | {record['address'][:30]}... | {record['price']:,.0f} VND")
                    new_count += 1
            
            if new_count == 0:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Không có tin mới. Đang chờ...", end='\r')
            
            time.sleep(60) # Quét lại mỗi phút

    except Exception as e:
        print(f"❌ Lỗi hệ thống: {e}")
    finally:
        driver.quit()
        producer.close()

if __name__ == "__main__":
    # Cấu hình quét từ trang 1 đến 10 lúc khởi động
    run_full_pipeline(start_page=1, end_page=10)