from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum, avg, expr
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, 
    BooleanType, LongType, TimestampType
)
import os
import sys

# ==========================================
# 1. CẤU HÌNH MÔI TRƯỜNG (WINDOWS FIX)
# ==========================================
os.environ['HADOOP_HOME'] = "D:\\hadoop" 
sys.path.append("D:\\hadoop\\bin")
os.environ['JAVA_HOME'] = "C:\\Program Files\\Eclipse Adoptium\\jdk-11.0.29.7-hotspot"
os.environ['PATH'] = os.environ['JAVA_HOME'] + "\\bin;" + os.environ['PATH']

# Fix lỗi Worker connection trên Windows
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

def create_spark_session():
    spark_packages = (
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,"
        "org.elasticsearch:elasticsearch-spark-30_2.12:8.11.1,"
        "com.datastax.spark:spark-cassandra-connector_2.12:3.3.0"
    )

    return (SparkSession.builder
            .appName("ZillowStreaming_Kafka_ES_Cassandra")
            .master("local[*]")
            .config("spark.jars.packages", spark_packages)
            
            # --- CẤU HÌNH ỔN ĐỊNH (TANK CONFIG) ---
            .config("spark.driver.memory", "4g")
            .config("spark.network.timeout", "600s")
            .config("spark.executor.heartbeatInterval", "120s")
            .config("spark.sql.streaming.checkpointLocation", "D:/chk_point/main")
            
            # --- ELASTICSEARCH ---
            .config("es.nodes", "localhost")
            .config("es.port", "9200")
            .config("es.nodes.wan.only", "true")
            
            # --- CASSANDRA ---
            .config("spark.cassandra.connection.host", "localhost")
            .config("spark.cassandra.connection.port", "9042")
            .getOrCreate())

def define_input_schema():
    # Schema khớp hoàn toàn với mock_producer.py
    listing_subtype_schema = StructType([
        StructField("is_FSBA", BooleanType(), True),
        StructField("is_openHouse", BooleanType(), True),
        StructField("is_newHome", BooleanType(), True)
    ])

    return StructType([
        StructField("timestamp", TimestampType(), True), # Spark tự cast Long sang Timestamp
        StructField("zpid", StringType(), True),
        StructField("city", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("bedrooms", IntegerType(), True),
        StructField("bathrooms", IntegerType(), True),
        StructField("livingArea", IntegerType(), True),
        StructField("lotAreaValue", DoubleType(), True),
        StructField("homeType", StringType(), True),
        StructField("homeStatus", StringType(), True),
        StructField("description", StringType(), True), # [QUAN TRỌNG] Cho NLP
        
        # Các trường phụ khác
        StructField("currency", StringType(), True),
        StructField("zestimate", IntegerType(), True),
        StructField("isFeatured", BooleanType(), True),
        StructField("isShowcaseListing", BooleanType(), True),
        StructField("listingSubType", listing_subtype_schema, True),
        StructField("address", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("newConstructionType", StringType(), True)
    ])

def setup_kafka_streaming(spark, kafka_brokers, kafka_topic):
    streaming_df = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "latest") # Đổi thành latest để chỉ lấy dữ liệu mới
        .option("failOnDataLoss", "false")
        .load())
    
    parsed_df = (streaming_df
        .select(from_json(col("value").cast("string"), define_input_schema()).alias("data"))
        .select("data.*")
    )
    return parsed_df

def process_data_for_elasticsearch(streaming_df):
    """
    Tính toán thống kê theo phút cho Kibana Dashboard
    """
    clean_df = streaming_df.filter(col("timestamp").isNotNull())
    
    return (clean_df
        .withWatermark("timestamp", "1 minutes")
        .groupBy(window("timestamp", "1 minutes"), "city")
        .agg(
            avg("price").alias("average_price"),
            sum("livingArea").alias("total_living_area"),
            avg("zestimate").alias("average_zestimate"),
            sum("bedrooms").alias("total_bedrooms")
        )
        # Làm phẳng window để ES dễ hiển thị
        .withColumn("window_start", col("window.start"))
        .drop("window")
    )

def process_data_for_cassandra(streaming_df):
    """
    Chuẩn bị dữ liệu thô cho Spark ML và Dashboard tìm kiếm
    """
    return (streaming_df
        .withColumn("listingsubtype_is_newhome", col("listingSubType.is_newHome"))
        # CHỌN VÀ ĐỔI TÊN CỘT VỀ CHỮ THƯỜNG (Lowercase) ĐỂ KHỚP CASSANDRA
        .select(
            col("zpid"),
            col("city"),
            col("price"),
            col("bedrooms"),
            col("bathrooms"),
            col("livingArea").alias("livingarea"), # Cassandra thường lưu chữ thường
            col("lotAreaValue").alias("lotareavalue"),
            col("homeType").alias("hometype"),
            col("newConstructionType").alias("newconstructiontype"),
            col("description"), # Cực quan trọng cho NLP
            col("isFeatured").alias("isfeatured"),
            col("isShowcaseListing").alias("isshowcaselisting"),
            col("listingsubtype_is_newhome")
        )
        .filter(col("zpid").isNotNull())
    )

def write_streaming_output(es_df, cassandra_df):
    # 1. Elasticsearch Writer
    query_es = (es_df.writeStream
        .outputMode("append")
        .format("org.elasticsearch.spark.sql")
        .option("es.resource", "zillow_stats/_doc")
        .option("checkpointLocation", "D:/chk_point/es")
        .trigger(processingTime='10 seconds')
        .start())
    
    # 2. Cassandra Writer
    query_cassandra = (cassandra_df.writeStream
        .outputMode("append")
        .format("org.apache.spark.sql.cassandra")
        .option("keyspace", "finaldata1")
        .option("table", "data2")
        .option("checkpointLocation", "D:/chk_point/cassandra")
        .trigger(processingTime='10 seconds')
        .start())

    # 3. Console Writer (Để debug)
    query_console = (cassandra_df.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", "false")
        .option("checkpointLocation", "D:/chk_point/console")
        .trigger(processingTime='10 seconds')
        .start())
    
    return [query_es, query_cassandra, query_console]

def main():
    KAFKA_BROKERS = "localhost:9097,localhost:9098,localhost:9099"
    KAFKA_TOPIC = "example_topic"
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print(">>> [STREAM] Đang khởi tạo Consumer...")
    print(">>> [INFO] Mode: Latest Offsets (Chỉ nhận dữ liệu mới)")
    
    raw_df = setup_kafka_streaming(spark, KAFKA_BROKERS, KAFKA_TOPIC)
    
    # Nhánh 1: Elastic (Thống kê)
    es_df = process_data_for_elasticsearch(raw_df)

    # Nhánh 2: Cassandra (Dữ liệu thô cho ML/Dashboard)
    cassandra_df = process_data_for_cassandra(raw_df)
    
    # Kích hoạt Stream
    queries = write_streaming_output(es_df, cassandra_df)
    
    print(">>> [READY] Hệ thống đang chạy. Hãy chạy mock_producer.py để bơm dữ liệu!")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()