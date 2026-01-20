import json
import logging
import os
from datetime import datetime
from kafka import KafkaConsumer
from hdfs import InsecureClient
import time
import sys

# ==========================================
# CẤU HÌNH LOGGING
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('kafka_hdfs_consumer.log', encoding='utf-8'), 
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# ==========================================
# CẤU HÌNH KẾT NỐI
# ==========================================
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9097,localhost:9098,localhost:9099').split(',')
KAFKA_TOPIC = 'example_topic'
KAFKA_GROUP_ID = 'hdfs_archiver_group' 

# HDFS: Cổng 9870 (WebHDFS)
HDFS_HOST = os.getenv('HDFS_HOST', 'http://localhost:9870')
HDFS_USER = 'root' 
HDFS_BASE_PATH = '/data/kafka_messages'

# BATCH CONFIG
BATCH_SIZE = 10       
BATCH_TIMEOUT = 60    

def setup_hdfs_client():
    try:
        client = InsecureClient(HDFS_HOST, user=HDFS_USER)
        # Test kết nối ngay khi khởi tạo
        client.content('/')
        logger.info(f"✅ Kết nối HDFS thành công tại: {HDFS_HOST}")
        return client
    except Exception as e:
        logger.error(f"❌ Failed to create HDFS client: {e}")
        raise

def create_hdfs_path(hdfs_client, date_path):
    """Hàm tạo thư mục an toàn"""
    try:
        if not hdfs_client.status(date_path, strict=False):
            hdfs_client.makedirs(date_path)
            logger.info(f"Created HDFS directory: {date_path}")
    except Exception as e:
        pass 

def save_batch_to_hdfs(hdfs_client, batch_messages, base_path):
    if not batch_messages:
        return

    try:
        current_time = datetime.now()
        
        # --- [SỬA QUAN TRỌNG] ---
        # Không dùng os.path.join vì trên Windows nó sinh ra dấu "\" (Backslash).
        # HDFS trên Linux chỉ hiểu dấu "/" (Forward slash).
        # Ta ép kiểu string f-string để dùng dấu "/" chuẩn.
        
        year = current_time.strftime('%Y')
        month = current_time.strftime('%m')
        day = current_time.strftime('%d')
        
        # Tạo đường dẫn thư mục: /data/kafka_messages/2026/01/20
        date_path = f"{base_path}/{year}/{month}/{day}"
        
        # Tạo thư mục trên HDFS
        create_hdfs_path(hdfs_client, date_path)

        # Tạo tên file
        filename = f"batdongsan_{current_time.strftime('%H_%M_%S')}_{len(batch_messages)}.json"
        
        # Tạo đường dẫn đầy đủ file: /data/kafka_messages/2026/01/20/file.json
        full_path = f"{date_path}/{filename}"

        # GHI JSON LINES (NDJSON)
        data_str = "\n".join([json.dumps(msg, ensure_ascii=False) for msg in batch_messages])
        
        # Ghi vào HDFS (overwrite=True để tránh lỗi nếu trùng tên trong cùng 1 giây)
        with hdfs_client.write(full_path, encoding='utf-8', overwrite=True) as writer:
            writer.write(data_str)
        
        logger.info(f"💾 Đã lưu {len(batch_messages)} tin vào HDFS: {full_path}")
        
    except Exception as e:
        logger.error(f"❌ Lỗi ghi HDFS: {e}")

def kafka_hdfs_consumer():
    logger.info(f"🚀 Khởi động Consumer... Batch Size: {BATCH_SIZE}")
    
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=KAFKA_GROUP_ID,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
    except Exception as e:
        logger.error(f"❌ Không thể kết nối Kafka: {e}")
        return

    try:
        hdfs_client = setup_hdfs_client()
    except:
        return

    batch_messages = []
    last_batch_time = time.time()

    print(">>> HDFS Consumer đang chạy. Đang chờ dữ liệu từ Kafka...")

    try:
        while True:
            msg_pack = consumer.poll(timeout_ms=1000)

            for topic_partition, messages in msg_pack.items():
                for message in messages:
                    batch_messages.append(message.value)
            
            # Logic kiểm tra batch
            time_diff = time.time() - last_batch_time
            is_timeout = time_diff >= BATCH_TIMEOUT
            is_full = len(batch_messages) >= BATCH_SIZE

            if batch_messages and (is_timeout or is_full):
                logger.info(f"Trigger write: {len(batch_messages)} msgs (Timeout: {is_timeout}, Full: {is_full})")
                
                save_batch_to_hdfs(hdfs_client, batch_messages, HDFS_BASE_PATH)
                
                # Reset
                batch_messages = []
                last_batch_time = time.time()
                consumer.commit()
            
            if not batch_messages:
                last_batch_time = time.time()

    except KeyboardInterrupt:
        logger.info("🛑 Consumer stopped by user.")
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
    finally:
        if batch_messages:
            logger.info("Đang ghi dữ liệu còn sót lại trước khi tắt...")
            save_batch_to_hdfs(hdfs_client, batch_messages, HDFS_BASE_PATH)
        if 'consumer' in locals():
            consumer.close()
        logger.info("Consumer closed.")

if __name__ == "__main__":
    kafka_hdfs_consumer()