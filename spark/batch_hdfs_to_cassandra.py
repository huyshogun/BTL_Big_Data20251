from hdfs import InsecureClient
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    BooleanType, LongType, DoubleType
)
from datetime import datetime, timedelta
import os
import sys

# ==========================================
# 1. CẤU HÌNH MÔI TRƯỜNG
# ==========================================
os.environ['HADOOP_HOME'] = "D:\\hadoop" 
sys.path.append("D:\\hadoop\\bin")
os.environ['JAVA_HOME'] = "C:\\Program Files\\Eclipse Adoptium\\jdk-11.0.29.7-hotspot"
os.environ['PATH'] = os.environ['JAVA_HOME'] + "\\bin;" + os.environ['PATH']

# ==========================================
# 2. XỬ LÝ THAM SỐ NGÀY THÁNG (QUAN TRỌNG CHO AIRFLOW)
# ==========================================
# Logic: Airflow thường chạy job ngày hôm qua (T-1). 
# Nếu có tham số dòng lệnh (vd: 2024-01-12), dùng ngày đó.
# Nếu không, mặc định lấy ngày hôm qua.
if len(sys.argv) > 1:
    target_date_str = sys.argv[1] # Định dạng YYYY-MM-DD
    target_date = datetime.strptime(target_date_str, "%Y-%m-%d")
    print(f">>> [Airflow Mode] Đang xử lý dữ liệu ngày: {target_date_str}")
else:
    # Mặc định lấy ngày hôm qua (Yesterday) vì Batch Job thường chạy sau nửa đêm
    target_date = datetime.now() - timedelta(days=1)
    print(f">>> [Manual Mode] Đang xử lý dữ liệu hôm qua: {target_date.strftime('%Y-%m-%d')}")

year = target_date.strftime("%Y")
month = target_date.strftime("%m")
day = target_date.strftime("%d")

# ==========================================
# 3. KHỞI TẠO SPARK & HDFS
# ==========================================
spark = SparkSession.builder \
    .appName("Batch_HDFS_To_Cassandra") \
    .master("local[*]") \
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.3.0") \
    .config("spark.cassandra.connection.host", "localhost") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

hdfs_client = InsecureClient("http://localhost:9870", user="root")

base_path = f"/data/kafka_messages/{year}/{month}/{day}"
print(f">>> Đang quét HDFS: {base_path}")

try:
    file_name_list = hdfs_client.list(base_path)
    hdfs_path_list = [f"{base_path}/{file_name}" for file_name in file_name_list]
    print(f">>> Tìm thấy {len(hdfs_path_list)} file.")
except Exception as e:
    print(f"!!! Không tìm thấy dữ liệu ngày {year}-{month}-{day}: {e}")
    hdfs_path_list = []

# ==========================================
# 4. ĐỊNH NGHĨA SCHEMA
# ==========================================
listing_subtype_schema = StructType([
    StructField("is_FSBA", BooleanType(), True),
    StructField("is_openHouse", BooleanType(), True),
    StructField("is_newHome", BooleanType(), True)
])

json_schema = StructType([
    StructField("timestamp", LongType(), True),
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
    StructField("description", StringType(), True), # Thêm trường description
    StructField("listingSubType", listing_subtype_schema, True),
    StructField("priceChange", IntegerType(), True),
    StructField("datePriceChanged", LongType(), True),
    StructField("openHouse", StringType(), True),
    StructField("priceReduction", StringType(), True),
    StructField("unit", StringType(), True),
    StructField("newConstructionType", StringType(), True),
    StructField("videoCount", IntegerType(), True)
])

# ==========================================
# 5. XỬ LÝ & GHI
# ==========================================
all_data = []

if not hdfs_path_list:
    print(">>> Dữ liệu trống. Kết thúc.")
    spark.stop()
    sys.exit(0)

for hdfs_path in hdfs_path_list:
    try:
        with hdfs_client.read(hdfs_path) as reader:
            all_data.extend(json.loads(reader.read()))
    except:
        pass

if len(all_data) > 0:
    df = spark.createDataFrame(all_data, schema=json_schema)
    
    df_final = (df.withColumn("listingsubtype_is_newhome", col("listingSubType.is_newHome"))
        .withColumnRenamed("homeType", "hometype")
        .withColumnRenamed("lotAreaValue", "lotareavalue")
        .withColumnRenamed("livingArea", "livingarea")
        .withColumnRenamed("isFeatured", "isfeatured")
        .withColumnRenamed("isShowcaseListing", "isshowcaselisting")
        .withColumnRenamed("newConstructionType", "newconstructiontype")
        .select(
            "zpid", "city", "hometype", "newconstructiontype",
            "lotareavalue", "bathrooms", "bedrooms", "livingarea",
            "isfeatured", "isshowcaselisting", "listingsubtype_is_newhome",
            "price", "description" 
        )
        .filter(col("zpid").isNotNull())
    )

    print(">>> Ghi vào Cassandra...")
    try:
        df_final.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="data2", keyspace="finaldata1") \
            .save()
        print(">>> SUCCESS!")
    except Exception as e:
        print(f"!!! Error: {e}")
else:
    print(">>> Empty Data.")

spark.stop()