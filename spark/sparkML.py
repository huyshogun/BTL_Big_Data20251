from pyspark.sql import SparkSession
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder, Imputer, BucketedRandomProjectionLSH, MinMaxScaler, ElementwiseProduct
from pyspark.ml.linalg import Vectors
from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql.functions import col, lower, when, lit
from pyspark.sql.types import DoubleType, FloatType, IntegerType
import os
import sys

# ==========================================
# CẤU HÌNH MÔI TRƯỜNG & SPARK (Giữ nguyên cấu hình tối ưu cũ)
# ==========================================
os.environ['HADOOP_HOME'] = "D:\\hadoop"
sys.path.append("D:\\hadoop\\bin")
os.environ['JAVA_HOME'] = "C:\\Program Files\\Eclipse Adoptium\\jdk-11.0.29.7-hotspot"
os.environ['PATH'] = os.environ['JAVA_HOME'] + "\\bin;" + os.environ['PATH']

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

spark = SparkSession.builder \
    .appName("HanoiHousePrice_Training_Weighted") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.network.timeout", "600s") \
    .config("spark.executor.heartbeatInterval", "120s") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.sql.shuffle.partitions", "16") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.3.0") \
    .config("spark.cassandra.connection.host", "localhost") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==========================================
# 1. ĐỌC DỮ LIỆU
# ==========================================
print(">>> [1/5] Loading data...")
HDFS_PATH = "hdfs://localhost:9000/data/kafka_messages"
try:
    df_read = spark.read.json(f"{HDFS_PATH}/*/*/*/*.json")
    if df_read.rdd.isEmpty(): sys.exit(0)
    
    df_read = df_read.filter(col("price").isNotNull() & col("livingArea").isNotNull())
    df_read = df_read.repartition(16)
    df_read.cache()
    print(f"✅ Loaded: {df_read.count()} records.")
except Exception as e:
    print(e); sys.exit(1)

# ==========================================
# 2. FEATURE ENGINEERING
# ==========================================
print(">>> [2/5] Feature Engineering...")

# Xử lý NLP
if "description" not in df_read.columns: df_read = df_read.withColumn("description", lit(""))
df_processed = df_read.fillna({"description": ""}).withColumn("desc_lower", lower(col("description"))) \
    .withColumn("has_red_book", when(col("desc_lower").rlike("sổ đỏ|sổ hồng|chính chủ"), 1).otherwise(0)) \
    .withColumn("is_street_front", when(col("desc_lower").rlike("mặt tiền|mặt phố|kinh doanh"), 1).otherwise(0)) \
    .withColumn("is_wide_alley", when(col("desc_lower").rlike("xe hơi|ô tô|oto"), 1).otherwise(0))

# Định nghĩa các nhóm cột
categorical_columns = ['city', 'homeType', 'newConstructionType']
boolean_cols = ['isFeatured', 'isShowcaseListing', 'has_red_book', 'is_street_front', 'is_wide_alley']

# Tách riêng livingArea để xử lý trọng số
special_col = ['livingArea'] 
normal_numeric_cols = ['lotAreaValue', 'bathrooms', 'bedrooms'] # Đã bỏ livingArea ra khỏi đây

# Fillna & Cast
for c in categorical_columns:
    if c not in df_processed.columns: df_processed = df_processed.withColumn(c, lit("UNKNOWN"))
df_processed = df_processed.fillna({c: 'UNKNOWN' for c in categorical_columns})

all_nums = normal_numeric_cols + special_col + boolean_cols
for c in all_nums:
     if c not in df_processed.columns: df_processed = df_processed.withColumn(c, lit(0))
df_processed = df_processed.fillna(0, subset=all_nums)

for c in all_nums + ["price"]:
    df_processed = df_processed.withColumn(c, col(c).cast(DoubleType()))

# ==========================================
# 3. PIPELINE VỚI TRỌNG SỐ (WEIGHTING)
# ==========================================
print(">>> [3/5] Building Weighted Pipeline...")

stages = []

# A. Xử lý cột Diện tích (QUAN TRỌNG NHẤT)
# 1. Impute
imputer_special = Imputer(inputCols=special_col, outputCols=["livingArea_imputed"]).setStrategy("mean")
stages.append(imputer_special)

# 2. Vectorize riêng lẻ
assembler_special = VectorAssembler(inputCols=["livingArea_imputed"], outputCol="vec_living")
stages.append(assembler_special)

# 3. Scale về [0, 1]
scaler_special = MinMaxScaler(inputCol="vec_living", outputCol="vec_living_scaled")
stages.append(scaler_special)

# 4. NHÂN TRỌNG SỐ (Weighting)
# Nhân diện tích với 3.0 -> Khoảng giá trị sẽ là [0, 3]
# Trong khi các cột khác chỉ là [0, 1]. Diện tích sẽ chi phối khoảng cách.
living_weight = ElementwiseProduct(scalingVec=Vectors.dense([3.0]), inputCol="vec_living_scaled", outputCol="vec_living_weighted")
stages.append(living_weight)

# B. Xử lý các cột còn lại (Bình thường)
imputer_normal = Imputer(inputCols=normal_numeric_cols, outputCols=[f"{c}_imputed" for c in normal_numeric_cols]).setStrategy("mean")
stages.append(imputer_normal)

for c in categorical_columns:
    indexer = StringIndexer(inputCol=c, outputCol=f"{c}_index", handleInvalid="keep")
    encoder = OneHotEncoder(inputCol=f"{c}_index", outputCol=f"{c}_vec")
    stages += [indexer, encoder]

assembler_inputs_normal = [f"{c}_vec" for c in categorical_columns] + [f"{c}_imputed" for c in normal_numeric_cols] + boolean_cols
assembler_normal = VectorAssembler(inputCols=assembler_inputs_normal, outputCol="vec_others_raw")
stages.append(assembler_normal)

scaler_normal = MinMaxScaler(inputCol="vec_others_raw", outputCol="vec_others_scaled")
stages.append(scaler_normal)

# C. Gộp tất cả lại (Diện tích đã nhân hệ số + Các cột khác)
final_assembler = VectorAssembler(inputCols=["vec_others_scaled", "vec_living_weighted"], outputCol="features")
stages.append(final_assembler)

# D. Model Training
# Tăng maxIter lên 50 để học tốt hơn
gbt = GBTRegressor(featuresCol="features", labelCol="price", maxIter=50, seed=42)
stages.append(gbt)

pipeline = Pipeline(stages=stages)
model = pipeline.fit(df_processed)

# Lưu model
model_path = "file:///D:/BTL_Big_Data/Big_Data_20251/saved_models"
model.write().overwrite().save(model_path)
print(f"✅ Đã lưu Model Trọng số tại: {model_path}")

# ==========================================
# 4. RECOMMENDATIONS (LSH)
# ==========================================
print(">>> [4/5] Generating Recommendations...")

df_vectorized = model.transform(df_processed).select("zpid", "features", "city", "price", "livingArea", "bedrooms")
df_vectorized.cache()

# LSH sẽ tính khoảng cách dựa trên vector "features"
# Vì "vec_living_weighted" có biên độ lớn gấp 3 lần các cột khác
# Nên 2 nhà lệch nhau về diện tích sẽ có khoảng cách xa hơn nhiều -> Lọc chuẩn hơn
brp = BucketedRandomProjectionLSH(inputCol="features", outputCol="hashes", bucketLength=100.0, numHashTables=3)
lsh_model = brp.fit(df_vectorized)

df_recent = df_vectorized.limit(20)
rec_df = lsh_model.approxSimilarityJoin(df_recent, df_vectorized, 1.5, distCol="EuclideanDistance") \
    .filter(col("datasetA.zpid") != col("datasetB.zpid"))

final_rec = rec_df.select(
    col("datasetA.zpid").alias("source_id"),
    col("datasetB.zpid").alias("target_id"),
    col("datasetB.city").alias("city"),
    col("datasetB.price").alias("price"),
    col("datasetB.livingArea").cast(FloatType()).alias("livingarea"),
    col("datasetB.bedrooms").cast(IntegerType()).alias("bedrooms"),
    col("EuclideanDistance").cast(FloatType()).alias("distance")
)

print(">>> [5/5] Saving to Cassandra...")
try:
    final_rec.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="recommendations", keyspace="finaldata1") \
        .mode("append") \
        .save()
    print("✅ Xong.")
except Exception as e:
    print(f"❌ Lỗi: {e}")

spark.stop()