import json
import time
import subprocess
import os
import sys
from kafka import KafkaConsumer

# ==========================================
# CẤU HÌNH
# ==========================================
KAFKA_TOPIC = "example_topic"
BOOTSTRAP_SERVERS = ['localhost:9097', 'localhost:9098', 'localhost:9099']
BATCH_SIZE = 10  # Số lượng bản ghi để kích hoạt training (Bạn có thể đổi thành 100, 1000)
SPARK_ML_FILE = "sparkML.py" # Tên file ML cần chạy

# Đảm bảo đường dẫn Python đúng (tránh lỗi không tìm thấy thư viện)
PYTHON_EXECUTABLE = sys.executable 

def run_spark_ml_job():
    print(f"\n{'='*50}")
    print(f"⚡ Đã đủ {BATCH_SIZE} bản ghi mới. Kích hoạt Spark ML...")
    print(f"{'='*50}")
    
    # Chờ 5 giây để đảm bảo dữ liệu từ Kafka kịp trôi vào Cassandra
    # (Vì Spark Streaming cần vài giây để xử lý và ghi xuống DB)
    print("⏳ Đang đợi 5s để dữ liệu đồng bộ xuống Cassandra...")
    time.sleep(5)
    
    try:
        # Gọi lệnh chạy file sparkML.py
        # Sử dụng check=True để báo lỗi nếu file ML chạy thất bại
        subprocess.run([PYTHON_EXECUTABLE, SPARK_ML_FILE], check=True)
        print(f"\n✅ Training hoàn tất! Dashboard đã được cập nhật.")
    except subprocess.CalledProcessError as e:
        print(f"❌ Lỗi khi chạy Spark ML: {e}")
    except Exception as e:
        print(f"❌ Lỗi không xác định: {e}")

    print(f"\n👀 Đang tiếp tục lắng nghe Kafka...")

def start_watcher():
    print(f">>> Đang khởi động trình giám sát (Watcher)...")
    print(f">>> Mục tiêu: Cứ mỗi {BATCH_SIZE} tin nhắn sẽ chạy lại {SPARK_ML_FILE}")

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset='latest', # Chỉ tính tin nhắn mới từ lúc bật script này
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    message_count = 0

    print(">>> Sẵn sàng! Đang chờ Producer bắn tin...")
    
    for message in consumer:
        # In ra tin nhắn nhỏ gọn để biết có dữ liệu vào
        data = message.value
        print(f"[Watcher] Nhận tin: {data.get('address', 'Unknown')} | Count: {message_count + 1}/{BATCH_SIZE}")
        
        message_count += 1

        # Kiểm tra điều kiện Trigger
        if message_count >= BATCH_SIZE:
            run_spark_ml_job()
            message_count = 0 # Reset bộ đếm

if __name__ == "__main__":
    start_watcher()