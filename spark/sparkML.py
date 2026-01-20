from pyspark.sql import SparkSession
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder, Imputer, BucketedRandomProjectionLSH, MinMaxScaler
from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql.functions import col, lower, when, lit
from pyspark.sql.types import DoubleType, FloatType, IntegerType
import os
import sys

# ==========================================
# CẤU HÌNH MÔI TRƯỜNG (WINDOWS)
# ==========================================
os.environ['HADOOP_HOME'] = "D:\\hadoop"
sys.path.append("D:\\hadoop\\bin")
os.environ['JAVA_HOME'] = "C:\\Program Files\\Eclipse Adoptium\\jdk-11.0.29.7-hotspot"
os.environ['PATH'] = os.environ['JAVA_HOME'] + "\\bin;" + os.environ['PATH']

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

# ==========================================
# CẤU HÌNH SPARK (TỐI ƯU CHO 20GB RAM / 4 CORE)
# ==========================================
print(">>> Đang khởi tạo Spark Session...")

spark = SparkSession.builder \
    .appName("HanoiHousePrice_Training_Batch") \
    .master("local[*]") \
    \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    \
    .config("spark.network.timeout", "600s") \
    .config("spark.executor.heartbeatInterval", "120s") \
    \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.driver.maxResultSize", "2g") \
    \
    .config("spark.sql.shuffle.partitions", "16") \
    .config("spark.default.parallelism", "16") \
    \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
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
HDFS_PATH = "hdfs://localhost:9000/data/kafka_messages"

try:
    # Đọc dữ liệu
    df_read = spark.read.json(f"{HDFS_PATH}/*/*/*/*.json")
    
    if df_read.rdd.isEmpty():
        print("⚠️  CẢNH BÁO: Không có dữ liệu JSON nào.")
        spark.stop()
        sys.exit(0)

    # Lọc rác
    df_read = df_read.filter(col("price").isNotNull() & col("livingArea").isNotNull())
    
    # [QUAN TRỌNG] Repartition để chia nhỏ dữ liệu cho 4 Core xử lý đều, tránh bị lệch (Skew)
    df_read = df_read.repartition(16) 
    
    # Cache ngay sau khi đọc để các bước sau không phải đọc lại đĩa
    df_read.cache()
    print(f"✅  Đã load và Cache: {df_read.count()} bản ghi.")

except Exception as e:
    print(f"❌  LỖI ĐỌC HDFS: {e}")
    spark.stop()
    sys.exit(1)

# ==========================================
# 2. FEATURE ENGINEERING
# ==========================================
print(">>> [2/5] Feature Engineering...")

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

for c in categorical_columns:
    if c not in df_processed.columns: df_processed = df_processed.withColumn(c, lit("UNKNOWN"))
df_processed = df_processed.fillna({c: 'UNKNOWN' for c in categorical_columns})

for c in real_numeric_cols + boolean_cols:
     if c not in df_processed.columns: df_processed = df_processed.withColumn(c, lit(0))
df_processed = df_processed.fillna(0, subset=real_numeric_cols + boolean_cols)

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
assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features_raw") # Đổi tên thành raw
stages.append(assembler)

# [QUAN TRỌNG] Thêm MinMaxScaler để chuẩn hóa dữ liệu -> Giúp LSH tính toán nhẹ hơn rất nhiều
scaler = MinMaxScaler(inputCol="features_raw", outputCol="features")
stages.append(scaler)

gbt = GBTRegressor(featuresCol="features", labelCol="price", maxIter=20, seed=42)
stages.append(gbt)

pipeline = Pipeline(stages=stages)
model = pipeline.fit(df_processed)

# Lưu vào thư mục con để an toàn
model_path = "file:///D:/BTL_Big_Data/Big_Data_20251/saved_models"
model.write().overwrite().save(model_path)
print(f"✅  Đã lưu Model tại: {model_path}")

# ==========================================
# 4. RECOMMENDATIONS (TỐI ƯU CHO MÁY YẾU)
# ==========================================
print(">>> [4/5] Generating Recommendations (Self-Join)...")

# Transform và chỉ chọn các cột cần thiết để tiết kiệm RAM
df_vectorized = model.transform(df_processed).select("zpid", "features", "city", "price", "livingArea", "bedrooms")

# Cache lại dữ liệu đã vector hóa
df_vectorized.cache()
print(f"Data cached for LSH. Count: {df_vectorized.count()}")

# Tăng bucketLength (10.0 -> 100.0) để giảm số lượng Buckets -> Giảm va chạm -> Tính nhanh hơn
# Giảm numHashTables (5 -> 3) để giảm tải CPU
brp = BucketedRandomProjectionLSH(inputCol="features", outputCol="hashes", bucketLength=100.0, numHashTables=3)
lsh_model = brp.fit(df_vectorized)

# [QUAN TRỌNG] Chỉ lấy 10-20 căn nhà mới nhất để tìm gợi ý (Thay vì chạy cả nghìn căn)
df_recent = df_vectorized.limit(20)

# Chạy Join với threshold vừa phải
rec_df = lsh_model.approxSimilarityJoin(df_recent, df_vectorized, 1.5, distCol="EuclideanDistance") \
    .filter(col("datasetA.zpid") != col("datasetB.zpid"))

final_rec = rec_df.select(
    col("datasetA.zpid").alias("source_id"),
    col("datasetB.zpid").alias("target_id"),
    col("datasetB.city").alias("city"),
    col("datasetB.price").alias("price"), # Cassandra là double -> OK
    
    # Sửa 1: Đổi tên thành 'livingarea' (thường) và ép sang Float
    col("datasetB.livingArea").cast(FloatType()).alias("livingarea"),
    
    # Sửa 2: Ép sang Integer cho khớp với Cassandra
    col("datasetB.bedrooms").cast(IntegerType()).alias("bedrooms"),
    
    # distance là float trong Cassandra -> Ép sang Float
    col("EuclideanDistance").cast(FloatType()).alias("distance")
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