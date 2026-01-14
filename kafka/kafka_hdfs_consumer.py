import json
import logging
import os
from datetime import datetime
from kafka import KafkaConsumer
from hdfs import InsecureClient
import time
import sys

# Cấu hình lại logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        # QUAN TRỌNG: Thêm encoding='utf-8' vào đây
        logging.FileHandler('kafka_hdfs_consumer.log', encoding='utf-8'), 
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9097', 'localhost:9098', 'localhost:9099']
KAFKA_TOPIC = 'example_topic'
KAFKA_GROUP_ID = 'hdfs_archiver_group' # Đổi tên group để không tranh chấp với Spark

# HDFS configuration
HDFS_HOST = 'http://localhost:9870'
# Lưu ý: Nếu chạy Docker, user thường là 'root' hoặc 'hadoop'. 
# Nếu lỗi permission, thử đổi user tại đây.
HDFS_USER = 'root' 
HDFS_BASE_PATH = '/data/kafka_messages'

# ==========================================
# CẤU HÌNH BATCH CHO DỮ LIỆU CHẬM (REAL-TIME CRAWL)
# ==========================================
# Vì tin về chậm, ta cần chờ lâu hơn để gom đủ file to
BATCH_SIZE = 100       # Gom đủ 100 tin thì ghi
BATCH_TIMEOUT = 300    # Hoặc cứ 5 phút (300s) thì ghi 1 lần dù ít tin

def setup_hdfs_client():
    try:
        return InsecureClient(HDFS_HOST, user=HDFS_USER)
    except Exception as e:
        logger.error(f"Failed to create HDFS client: {e}")
        raise

def create_hdfs_path(hdfs_client, date_path):
    try:
        if not hdfs_client.status(date_path, strict=False):
            hdfs_client.makedirs(date_path)
            logger.info(f"Created HDFS directory: {date_path}")
    except Exception as e:
        logger.error(f"Error creating HDFS directory {date_path}: {e}")
        # Không raise error ở đây để tránh dừng consumer chỉ vì thư mục đã tồn tại
        pass 

def save_batch_to_hdfs(hdfs_client, batch_messages, base_path):
    if not batch_messages:
        return

    try:
        current_time = datetime.now()
        # Phân vùng theo Ngày/Tháng/Năm
        date_path = os.path.join(
            base_path,
            current_time.strftime('%Y'),
            current_time.strftime('%m'),
            current_time.strftime('%d')
        )
        create_hdfs_path(hdfs_client, date_path)

        # Tên file bao gồm thời gian chi tiết để không trùng
        filename = f"batdongsan_{current_time.strftime('%H_%M_%S')}_{len(batch_messages)}.json"
        full_path = os.path.join(date_path, filename)

        # Ghi dữ liệu với encoding utf-8 để hiển thị tiếng Việt
        with hdfs_client.write(full_path, encoding='utf-8') as writer:
            # dump từng dòng hoặc dump cả list tùy nhu cầu đọc
            # Ở đây dump cả list cho chuẩn JSON
            json.dump(batch_messages, writer, ensure_ascii=False) 
        
        logger.info(f">>> Đã lưu {len(batch_messages)} tin vào HDFS: {full_path}")
    except Exception as e:
        logger.error(f"Lỗi ghi HDFS: {e}")

def kafka_hdfs_consumer():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset='earliest',
        enable_auto_commit=False, # Commit thủ công để đảm bảo an toàn dữ liệu
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    hdfs_client = setup_hdfs_client()
    batch_messages = []
    last_batch_time = time.time()

    print(">>> HDFS Consumer đang chạy. Đang chờ dữ liệu từ Kafka...")

    try:
        while True:
            # Dùng poll() thay vì for-loop để code không bị block hoàn toàn
            # timeout_ms=1000: Chờ 1 giây xem có tin mới không
            msg_pack = consumer.poll(timeout_ms=1000)

            for topic_partition, messages in msg_pack.items():
                for message in messages:
                    batch_messages.append(message.value)
            
            # Kiểm tra điều kiện ghi file
            time_diff = time.time() - last_batch_time
            is_timeout = time_diff >= BATCH_TIMEOUT
            is_full = len(batch_messages) >= BATCH_SIZE

            # Chỉ ghi nếu (Có dữ liệu VÀ (Hết giờ HOẶC Đầy bộ nhớ))
            if batch_messages and (is_timeout or is_full):
                logger.info(f"Trigger write: {len(batch_messages)} msgs (Timeout: {is_timeout}, Full: {is_full})")
                
                save_batch_to_hdfs(hdfs_client, batch_messages, HDFS_BASE_PATH)
                
                # Reset
                batch_messages = []
                last_batch_time = time.time()
                consumer.commit() # Xác nhận đã xử lý xong
            
            # Cập nhật lại thời gian nếu batch rỗng để tránh timeout ảo
            if not batch_messages:
                last_batch_time = time.time()

    except KeyboardInterrupt:
        logger.info("Consumer stopped by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        # Ghi nốt dữ liệu còn sót lại trước khi tắt
        if batch_messages:
            save_batch_to_hdfs(hdfs_client, batch_messages, HDFS_BASE_PATH)
        consumer.close()

if __name__ == "__main__":
    kafka_hdfs_consumer()