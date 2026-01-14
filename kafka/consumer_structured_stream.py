from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum, avg
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, 
    BooleanType, LongType, TimestampType
)

import os
import sys

# ==========================================
# 1. CẤU HÌNH MÔI TRƯỜNG WINDOWS
# ==========================================
os.environ['HADOOP_HOME'] = "D:\\hadoop" 
sys.path.append("D:\\hadoop\\bin")

# Thay đường dẫn Java 11 của bạn vào đây
os.environ['JAVA_HOME'] = "C:\\Program Files\\Eclipse Adoptium\\jdk-11.0.29.7-hotspot"
os.environ['PATH'] = os.environ['JAVA_HOME'] + "\\bin;" + os.environ['PATH']

def create_spark_session():
    # Thêm spark-cassandra-connector vào packages
    spark_packages = (
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,"
        "org.elasticsearch:elasticsearch-spark-30_2.12:8.11.1,"
        "com.datastax.spark:spark-cassandra-connector_2.12:3.3.0" # Thêm dòng này
    )

    return (SparkSession.builder
            .appName("ZillowStreaming_Kafka_ES_Cassandra")
            .master("local[*]")
            .config("spark.jars.packages", spark_packages)
            .config("spark.driver.memory", "2g")
            .config("spark.sql.streaming.checkpointLocation", "D:/chk_point/main")
            # Cấu hình Elastic
            .config("es.nodes", "localhost")
            .config("es.port", "9200")
            .config("es.nodes.wan.only", "true")
            # Cấu hình Cassandra
            .config("spark.cassandra.connection.host", "localhost")
            .config("spark.cassandra.connection.port", "9042")
            .getOrCreate())

def define_input_schema():
    # Schema lồng nhau
    listing_subtype_schema = StructType([
        StructField("is_FSBA", BooleanType(), True),
        StructField("is_openHouse", BooleanType(), True),
        StructField("is_newHome", BooleanType(), True)
    ])

    return StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("zpid", StringType(), True),
        StructField("homeStatus", StringType(), True),
        StructField("detailUrl", StringType(), True),
        StructField("address", StringType(), True),
        StructField("streetAddress", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("country", StringType(), True),
        StructField("zipcode", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("homeType", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("zestimate", IntegerType(), True),
        StructField("rentZestimate", IntegerType(), True),
        StructField("taxAssessedValue", IntegerType(), True),
        StructField("lotAreaValue", DoubleType(), True),
        StructField("lotAreaUnit", StringType(), True),
        StructField("bathrooms", IntegerType(), True),
        StructField("bedrooms", IntegerType(), True),
        StructField("livingArea", IntegerType(), True),
        StructField("daysOnZillow", IntegerType(), True),
        StructField("isFeatured", BooleanType(), True),
        StructField("isPreforeclosureAuction", BooleanType(), True),
        StructField("timeOnZillow", IntegerType(), True),
        StructField("isNonOwnerOccupied", BooleanType(), True),
        StructField("isPremierBuilder", BooleanType(), True),
        StructField("isZillowOwned", BooleanType(), True),
        StructField("isShowcaseListing", BooleanType(), True),
        StructField("imgSrc", StringType(), True),
        StructField("hasImage", BooleanType(), True),
        StructField("brokerName", StringType(), True),
        StructField("listingSubType", listing_subtype_schema, True), # Nested
        StructField("priceChange", IntegerType(), True),
        StructField("datePriceChanged", LongType(), True),
        StructField("openHouse", StringType(), True),
        StructField("priceReduction", StringType(), True),
        StructField("unit", StringType(), True),
        StructField("newConstructionType", StringType(), True),
        StructField("videoCount", IntegerType(), True)
    ])

def setup_kafka_streaming(spark, kafka_brokers, kafka_topic):
    streaming_df = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load())
    
    parsed_df = (streaming_df
        .select(from_json(col("value").cast("string"), define_input_schema()).alias("data"))
        .select("data.*")
    )
    return parsed_df

def process_data_for_elasticsearch(streaming_df):
    # Logic cũ: Tính toán trung bình để vẽ biểu đồ
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
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .drop("window")
    )

def process_data_for_cassandra(streaming_df):
    # Logic mới: Làm phẳng dữ liệu để lưu vào Cassandra cho ML train
    # Cần tách listingSubType ra thành cột phẳng
    return (streaming_df
        .withColumn("listingsubtype_is_newhome", col("listingSubType.is_newHome"))
        # Chọn các cột cần thiết cho model ML
        .select(
            "zpid", "city", "hometype", "newconstructiontype",
            "lotareavalue", "bathrooms", "bedrooms", "livingarea",
            "isfeatured", "isshowcaselisting", "listingsubtype_is_newhome",
            "price"
        )
        # Lọc bỏ bản ghi null primary key để tránh lỗi Cassandra
        .filter(col("zpid").isNotNull())
    )

def write_streaming_output(es_df, cassandra_df):
    # 1. Ghi vào Elasticsearch
    query_es = (es_df.writeStream
        .outputMode("append")
        .format("org.elasticsearch.spark.sql")
        .option("es.resource", "zillow_stats/_doc")
        .option("checkpointLocation", "D:/chk_point/es")
        .trigger(processingTime='10 seconds') # Thêm dòng này: 10 giây mới xử lý 1 lần
        .start())
    
    # 2. Ghi vào Cassandra
    query_cassandra = (cassandra_df.writeStream
        .outputMode("append")
        .format("org.apache.spark.sql.cassandra")
        .option("keyspace", "finaldata1")
        .option("table", "data2")
        .option("checkpointLocation", "D:/chk_point/cassandra")
        .trigger(processingTime='10 seconds') # Thêm dòng này
        .start())

    # 3. Console
    query_console = (cassandra_df.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", "false")
        .option("checkpointLocation", "D:/chk_point/console")
        .trigger(processingTime='10 seconds') # Thêm dòng này
        .start())
    
    return [query_es, query_cassandra, query_console]

def main():
    KAFKA_BROKERS = "localhost:9097,localhost:9098,localhost:9099"
    KAFKA_TOPIC = "example_topic"
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print(">>> Khởi tạo Stream...")
    raw_df = setup_kafka_streaming(spark, KAFKA_BROKERS, KAFKA_TOPIC)
    
    # Nhánh 1: Xử lý cho Elastic (Aggregation)
    es_df = process_data_for_elasticsearch(raw_df)

    # Nhánh 2: Xử lý cho Cassandra (Flatten & Selection)
    cassandra_df = process_data_for_cassandra(raw_df)
    
    # Ghi dữ liệu
    queries = write_streaming_output(es_df, cassandra_df)
    
    print(">>> Stream đang chạy. Dữ liệu đang vào ES và Cassandra...")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()