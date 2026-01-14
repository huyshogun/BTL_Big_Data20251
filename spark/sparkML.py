from pyspark.sql import SparkSession
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder, Imputer, BucketedRandomProjectionLSH
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, abs, lower, when, lit
from pyspark.sql.types import DoubleType, IntegerType, StringType

import os
import sys

# ==========================================
# 1. CẤU HÌNH MÔI TRƯỜNG
# ==========================================
os.environ['HADOOP_HOME'] = "D:\\hadoop" 
sys.path.append("D:\\hadoop\\bin")
os.environ['JAVA_HOME'] = "C:\\Program Files\\Eclipse Adoptium\\jdk-11.0.29.7-hotspot"
os.environ['PATH'] = os.environ['JAVA_HOME'] + "\\bin;" + os.environ['PATH']

spark = SparkSession.builder \
    .appName("HanoiHousePrice_Advanced_NLP_RecSys") \
    .master("local[*]") \
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.3.0") \
    .config("spark.cassandra.connection.host", "localhost") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==========================================
# 2. ĐỌC DỮ LIỆU TỪ CASSANDRA
# ==========================================
print(">>> [1/6] Loading data from Cassandra...")
try:
    df_read = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="data2", keyspace="finaldata1") \
        .load()
except Exception as e:
    print(f"!!! Lỗi đọc Cassandra: {e}")
    sys.exit(1)

# ==========================================
# 3. FEATURE ENGINEERING (NLP + CLEANING)
# ==========================================
print(">>> [2/6] Advanced Feature Engineering (NLP Extraction)...")

# 3.1: Xử lý cột Description (Nếu null thì để chuỗi rỗng)
# Lưu ý: Nếu Cassandra chưa có cột description, Spark sẽ báo lỗi. Hãy chắc chắn đã chạy Bước 1.
if "description" not in df_read.columns:
    print("!!! Cảnh báo: Không tìm thấy cột 'description'. Đang tạo cột giả lập...")
    df_read = df_read.withColumn("description", lit("Không có mô tả"))

df_nlp = df_read.fillna({"description": ""})

# 3.2: NLP - Trích xuất đặc trưng từ văn bản (Keyword Extraction)
# Chuyển về chữ thường để dễ so sánh
df_nlp = df_nlp.withColumn("desc_lower", lower(col("description")))

# Tạo 3 tính năng mới (Binary Features)
df_processed = df_nlp \
    .withColumn("has_red_book", when(col("desc_lower").rlike("sổ đỏ|sổ hồng|chính chủ"), 1).otherwise(0)) \
    .withColumn("is_street_front", when(col("desc_lower").rlike("mặt tiền|mặt phố|kinh doanh"), 1).otherwise(0)) \
    .withColumn("is_wide_alley", when(col("desc_lower").rlike("xe hơi|ô tô|oto"), 1).otherwise(0))

print("    -> Đã trích xuất xong: has_red_book, is_street_front, is_wide_alley")

# ==========================================
# 4. CHUẨN BỊ DỮ LIỆU TRAIN
# ==========================================
print(">>> [3/6] Cleaning & Type Casting...")

categorical_columns = ['city', 'hometype', 'newconstructiontype']
real_numeric_cols = ['lotareavalue', 'bathrooms', 'bedrooms', 'livingarea']
# Thêm các cột NLP mới vào danh sách cột Boolean
boolean_cols = ['isfeatured', 'isshowcaselisting', 'listingsubtype_is_newhome', 
                'has_red_book', 'is_street_front', 'is_wide_alley']

all_numeric_cols = real_numeric_cols + boolean_cols

# Fill NA
df_processed = df_processed.fillna({c: 'UNKNOWN' for c in categorical_columns})
fill_values = {c: 0 for c in boolean_cols}
fill_values.update({c: 0.0 for c in real_numeric_cols})
df_processed = df_processed.fillna(fill_values)

# Cast types
for col_name in boolean_cols:
    df_processed = df_processed.withColumn(col_name, col(col_name).cast(IntegerType()).cast(DoubleType()))
for col_name in real_numeric_cols:
    df_processed = df_processed.withColumn(col_name, col(col_name).cast(DoubleType()))

df_processed = df_processed.withColumn("price", col("price").cast(DoubleType()))

# ==========================================
# 5. XÂY DỰNG PIPELINE (GBT REGRESSOR)
# ==========================================
print(">>> [4/6] Building Pipeline & Training...")

stages = []

# Imputer
imputer = Imputer(inputCols=all_numeric_cols, outputCols=[f"{c}_imputed" for c in all_numeric_cols]).setStrategy("mean")
stages.append(imputer)

# Encoder
for col_name in categorical_columns:
    indexer = StringIndexer(inputCol=col_name, outputCol=f"{col_name}_index", handleInvalid="keep")
    encoder = OneHotEncoder(inputCol=f"{col_name}_index", outputCol=f"{col_name}_vec")
    stages += [indexer, encoder]

# Vector Assembler
assembler_inputs = [f"{c}_vec" for c in categorical_columns] + [f"{c}_imputed" for c in all_numeric_cols]
assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features")
stages.append(assembler)

# Model GBT
gbt = GBTRegressor(featuresCol="features", labelCol="price", maxIter=50, stepSize=0.1, seed=42)
stages.append(gbt)

pipeline = Pipeline(stages=stages)

# Split & Train
(train_data, test_data) = df_processed.randomSplit([0.8, 0.2], seed=42)
train_data.cache()
test_data.cache()

model = pipeline.fit(train_data)
predictions = model.transform(test_data)

# Evaluate
evaluator_r2 = RegressionEvaluator(labelCol="price", predictionCol="prediction", metricName="r2")
r2 = evaluator_r2.evaluate(predictions)
print(f"\n📊 [KẾT QUẢ DỰ BÁO] R2 Score (với NLP Features): {r2:.4f}")

# ==========================================
# 6. HỆ THỐNG GỢI Ý (RECOMMENDATION SYSTEM)
# ==========================================
print("\n" + "="*50)
print("🏠 TÍNH NĂNG MỚI: HỆ THỐNG GỢI Ý NHÀ TƯƠNG TỰ (LSH)")
print("="*50)

# Bước 6.1: Chuẩn bị model LSH (Locality Sensitive Hashing)
# LSH giúp tìm kiếm vector tương tự cực nhanh trong không gian nhiều chiều
# Chúng ta dùng vector 'features' đã được tạo ra bởi Pipeline trên
# Tuy nhiên, Pipeline model trả về 'predictions' đã có cột 'features', ta dùng luôn nó.

# Lấy dữ liệu đã transform xong (chứa cột features)
df_vectorized = model.transform(df_processed)

# Khởi tạo LSH
# bucketLength: Độ rộng của bucket (càng nhỏ càng chính xác nhưng chậm)
# numHashTables: Số lượng bảng băm
brp = BucketedRandomProjectionLSH(inputCol="features", outputCol="hashes", bucketLength=100.0, numHashTables=5)

# Fit mô hình LSH
print(">>> Training LSH Model...")
model_lsh = brp.fit(df_vectorized)

# Bước 6.2: Demo tìm nhà tương tự
# Lấy căn nhà đầu tiên trong tập Test làm ví dụ "Nhà đang xem"
print(">>> Đang chọn một căn nhà mẫu từ tập Test...")
target_house = model.transform(test_data).first()
target_features = target_house['features']

print(f"--- Căn nhà đang xem ---")
print(f"Địa chỉ: {target_house['city']}")
print(f"Giá: {target_house['price']/1e9:.2f} Tỷ | DT: {target_house['livingarea']} m2 | PN: {target_house['bedrooms']}")
if target_house['has_red_book'] == 1: print("✅ Có Sổ đỏ/Chính chủ")
if target_house['is_street_front'] == 1: print("✅ Mặt tiền/Kinh doanh")

# Bước 6.3: Tìm 5 hàng xóm gần nhất (Nearest Neighbors)
print("\n>>> 🔍 Top 5 Căn nhà tương tự nhất (Dựa trên AI):")
similar_houses = model_lsh.approxNearestNeighbors(df_vectorized, target_features, 5)

# Hiển thị kết quả
print(">>> [6/6] Exporting Recommendation Results...")

# 1. Tính toán thêm cột "Do_Giong_Nhau" (Similarity Score) ngay trong Spark
# Công thức: Khoảng cách càng nhỏ -> Độ giống càng cao (Max 100%)
# Cast sang Integer để số đẹp (VD: 95, 80...)
rec_final = similar_houses \
    .withColumn("Do_Giong_Nhau", (1 / (1 + col("distCol")) * 100).cast("int")) \
    .withColumn("price_Ty", (col("price")/1e9).cast("decimal(10,2)")) \
    .orderBy("distCol") # Sắp xếp để căn giống nhất lên đầu

# 2. In ra màn hình để kiểm tra (Debug)
print("--- Preview dữ liệu gợi ý ---")
rec_final.select("city", "livingarea", "price_Ty", "has_red_book", "is_street_front", "Do_Giong_Nhau").show()

# 3. Xuất ra CSV
try:
    # Chọn đầy đủ các cột mà Dashboard cần
    rec_export = rec_final.select(
        "city", 
        "livingarea", 
        "bedrooms", 
        "price", 
        "has_red_book", 
        "is_street_front", 
        "distCol",
        "Do_Giong_Nhau" # Thêm cột này vào file
    )
    
    # Chuyển sang Pandas & Lưu CSV
    rec_pandas = rec_export.toPandas()
    rec_pandas.to_csv("D:/BTL_Big_Data/Big_Data_20251/recommendation_results.csv", index=False)
    print("-> Đã lưu file gợi ý thành công: D:/BTL_Big_Data/recommendation_results.csv")
    
except Exception as e:
    print(f"!!! Lỗi xuất file gợi ý: {e}")
    
print("Chú thích: 'distCol' càng nhỏ nghĩa là càng giống căn gốc.")

# ==========================================
# 7. XUẤT FILE CHO DASHBOARD
# ==========================================
# (Đoạn code xuất CSV cũ giữ nguyên ở đây để vẽ dashboard)
output_df = predictions.select("city", "livingarea", "bedrooms", "bathrooms", "price", "prediction")
pandas_df = output_df.toPandas()
pandas_df.to_csv("D:/BTL_Big_Data/Big_Data_20251/prediction_results.csv", index=False)
print("\n>>> Đã xuất file kết quả Dashboard.")

spark.stop()