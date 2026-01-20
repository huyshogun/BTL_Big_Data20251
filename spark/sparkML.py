from pyspark.sql import SparkSession
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder, Imputer, BucketedRandomProjectionLSH
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, lower, when, lit
from pyspark.sql.types import DoubleType
import os
import sys

# ==========================================
# CẤU HÌNH MÔI TRƯỜNG
# ==========================================
# Đảm bảo đường dẫn này đúng với máy bạn
os.environ['HADOOP_HOME'] = "D:\\hadoop" 
sys.path.append("D:\\hadoop\\bin")
os.environ['JAVA_HOME'] = "C:\\Program Files\\Eclipse Adoptium\\jdk-11.0.29.7-hotspot"
os.environ['PATH'] = os.environ['JAVA_HOME'] + "\\bin;" + os.environ['PATH']

# --- THÊM ĐOẠN NÀY VÀO ĐẦU FILE ---

# 1. Trỏ đúng đường dẫn Python đang chạy (trong venv)
# Giúp Spark biết chính xác dùng python.exe nào để khởi tạo worker
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# 2. Ép buộc mọi giao tiếp nội bộ phải qua localhost
# Tránh việc Spark chọn nhầm IP của Docker (172.x.x.x)
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

# Khởi tạo Spark
print(">>> Đang khởi tạo Spark Session...")
# Khởi tạo Spark Session (Cập nhật thêm timeout)
spark = SparkSession.builder \
    .appName("HanoiHousePrice_Training_Batch") \
    .master("local[*]") \
    \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    \
    .config("spark.network.timeout", "300s") \
    .config("spark.executor.heartbeatInterval", "60s") \
    \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem") \
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
    \
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.3.0") \
    .config("spark.cassandra.connection.host", "localhost") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==========================================
# 1. ĐỌC DỮ LIỆU TỪ HDFS
# ==========================================
print(">>> [1/5] Loading data from HDFS...")

# Đường dẫn HDFS (Vì đã cấu hình defaultFS ở trên, ta có thể dùng đường dẫn tuyệt đối)
HDFS_PATH = "hdfs://localhost:9000/data/kafka_messages"

try:
    # Kỹ thuật dùng Java Gateway để kiểm tra file tồn tại an toàn
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    exists = fs.exists(spark._jvm.org.apache.hadoop.fs.Path(HDFS_PATH))
    
    if not exists:
        print(f"⚠️  CẢNH BÁO: Thư mục {HDFS_PATH} chưa tồn tại trên HDFS.")
        print("    -> Lý do: Consumer chưa ghi kịp dữ liệu.")
        print("    -> Giải pháp: Chạy Consumer và gửi thêm tin nhắn Kafka.")
        spark.stop()
        sys.exit(0)

    # Đọc dữ liệu: Dùng wildcard ** để quét mọi thư mục con
    df_read = spark.read.json(f"{HDFS_PATH}/*/*/*/*.json")
    
    # Kiểm tra rỗng
    if df_read.rdd.isEmpty():
        print("⚠️  CẢNH BÁO: Thư mục có tồn tại nhưng KHÔNG CÓ FILE JSON nào.")
        spark.stop()
        sys.exit(0)

    # Lọc dữ liệu rác
    count_before = df_read.count()
    df_read = df_read.filter(col("price").isNotNull() & col("livingArea").isNotNull())
    print(f"✅  Đã load thành công {df_read.count()}/{count_before} bản ghi từ HDFS.")

except Exception as e:
    # Bắt lỗi Wrong FS hoặc lỗi kết nối
    print(f"❌  LỖI ĐỌC HDFS: {e}")
    spark.stop()
    sys.exit(1)

# ==========================================
# 2. FEATURE ENGINEERING
# ==========================================
print(">>> [2/5] Feature Engineering...")

# (Giữ nguyên logic cũ của bạn)
if "description" not in df_read.columns:
    df_read = df_read.withColumn("description", lit(""))

df_nlp = df_read.fillna({"description": ""}).withColumn("desc_lower", lower(col("description")))

df_processed = df_nlp \
    .withColumn("has_red_book", when(col("desc_lower").rlike("sổ đỏ|sổ hồng|chính chủ"), 1).otherwise(0)) \
    .withColumn("is_street_front", when(col("desc_lower").rlike("mặt tiền|mặt phố|kinh doanh"), 1).otherwise(0)) \
    .withColumn("is_wide_alley", when(col("desc_lower").rlike("xe hơi|ô tô|oto"), 1).otherwise(0))

categorical_columns = ['city', 'homeType', 'newConstructionType']
real_numeric_cols = ['lotAreaValue', 'bathrooms', 'bedrooms', 'livingArea']
boolean_cols = ['isFeatured', 'isShowcaseListing', 'has_red_book', 'is_street_front', 'is_wide_alley']

# Xử lý thiếu cột và fillna
for c in categorical_columns:
    if c not in df_processed.columns: df_processed = df_processed.withColumn(c, lit("UNKNOWN"))
df_processed = df_processed.fillna({c: 'UNKNOWN' for c in categorical_columns})

for c in real_numeric_cols + boolean_cols:
     if c not in df_processed.columns: df_processed = df_processed.withColumn(c, lit(0))
df_processed = df_processed.fillna(0, subset=real_numeric_cols + boolean_cols)

# Cast types
for col_name in boolean_cols + real_numeric_cols:
    df_processed = df_processed.withColumn(col_name, col(col_name).cast(DoubleType()))
df_processed = df_processed.withColumn("price", col("price").cast(DoubleType()))

# ==========================================
# 3. TRAINING & SAVING
# ==========================================
print(">>> [3/5] Training & Saving Model...")

stages = []
imputer = Imputer(inputCols=real_numeric_cols, outputCols=[f"{c}_imputed" for c in real_numeric_cols]).setStrategy("mean")
stages.append(imputer)

for col_name in categorical_columns:
    indexer = StringIndexer(inputCol=col_name, outputCol=f"{col_name}_index", handleInvalid="keep")
    encoder = OneHotEncoder(inputCol=f"{col_name}_index", outputCol=f"{col_name}_vec")
    stages += [indexer, encoder]

assembler_inputs = [f"{c}_vec" for c in categorical_columns] + [f"{c}_imputed" for c in real_numeric_cols] + boolean_cols
assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features")
stages.append(assembler)

gbt = GBTRegressor(featuresCol="features", labelCol="price", maxIter=20, seed=42)
stages.append(gbt)

pipeline = Pipeline(stages=stages)
model = pipeline.fit(df_processed)

# Lưu model (Overwrite để ghi đè model cũ)
model_path = "file:///D:/BTL_Big_Data/model"
model.write().overwrite().save(model_path)
print(f"✅  Đã lưu Model tại: {model_path}")

# ==========================================
# 4. RECOMMENDATIONS (Cassandra)
# ==========================================
print(">>> [4/5] Generating Recommendations (Self-Join)...")

df_vectorized = model.transform(df_processed)
brp = BucketedRandomProjectionLSH(inputCol="features", outputCol="hashes", bucketLength=100.0, numHashTables=5)
lsh_model = brp.fit(df_vectorized)

# Lấy mẫu nhỏ để demo (Top 200)
df_recent = df_vectorized.limit(200) 
rec_df = lsh_model.approxSimilarityJoin(df_recent, df_vectorized, 2.0, distCol="EuclideanDistance") \
    .filter(col("datasetA.zpid") != col("datasetB.zpid"))

final_rec = rec_df.select(
    col("datasetA.zpid").alias("source_id"),
    col("datasetB.zpid").alias("target_id"),
    col("datasetB.city").alias("city"),
    col("datasetB.price").alias("price"),
    col("datasetB.livingArea").alias("livingArea"),
    col("datasetB.bedrooms").alias("bedrooms"),
    col("EuclideanDistance").alias("distance")
)

print(">>> [5/5] Saving to Cassandra...")
try:
    final_rec.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="recommendations", keyspace="finaldata1") \
        .mode("append") \
        .save()
    print("✅  Đã lưu xong vào Cassandra.")
except Exception as e:
    print(f"❌  Lỗi lưu Cassandra: {e}")

spark.stop()