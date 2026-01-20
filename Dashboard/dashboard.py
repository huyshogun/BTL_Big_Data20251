import streamlit as st
import pandas as pd
import plotly.express as px
import os
import sys

# Cấu hình PySpark để chạy ngầm trong Streamlit
# (Cần thiết để load model và connect Cassandra)
os.environ['HADOOP_HOME'] = "D:\\hadoop"
sys.path.append("D:\\hadoop\\bin")
os.environ['JAVA_HOME'] = "C:\\Program Files\\Eclipse Adoptium\\jdk-11.0.29.7-hotspot"
os.environ['PATH'] = os.environ['JAVA_HOME'] + "\\bin;" + os.environ['PATH']

from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# ==========================================
# 1. SETUP SPARK SESSION (CACHE RESOURCE)
# ==========================================
@st.cache_resource
def get_spark_session():
    """Khởi tạo Spark 1 lần duy nhất cho cả app"""
    return SparkSession.builder \
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

@st.cache_resource
def load_trained_model(_spark):
    """Load model đã train từ disk"""
    try:
        model_path = "file:///D:/BTL_Big_Data/model"
        return PipelineModel.load(model_path)
    except Exception as e:
        return None

# Khởi tạo
spark = get_spark_session()
model = load_trained_model(spark)

# ==========================================
# 2. HÀM TRUY VẤN DỮ LIỆU
# ==========================================
def get_dashboard_stats():
    """Lấy thống kê tổng hợp từ Cassandra (Bảng data2)"""
    try:
        # Đọc bảng data2 từ Cassandra
        df_cass = spark.read \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="data2", keyspace="finaldata1") \
            .load()
        
        # Tạo View để query SQL cho lẹ
        df_cass.createOrReplaceTempView("houses")
        
        # Query thống kê
        stats = spark.sql("""
            SELECT 
                city, 
                COUNT(*) as count, 
                AVG(price) as avg_price, 
                AVG(livingarea) as avg_area,
                MAX(price) as max_price
            FROM houses 
            WHERE city IS NOT NULL 
            GROUP BY city
        """).toPandas()
        
        return stats
    except Exception as e:
        st.error(f"Lỗi đọc Cassandra: {e}")
        return pd.DataFrame()

def predict_custom_house(input_data):
    """Dự đoán giá cho input nhập tay"""
    if model is None:
        return 0.0
    
    # Tạo DataFrame từ input dictionary
    # Cần đúng schema như lúc train
    schema = StructType([
        StructField("city", StringType(), True),
        StructField("homeType", StringType(), True),
        StructField("newConstructionType", StringType(), True),
        StructField("lotAreaValue", DoubleType(), True),
        StructField("bathrooms", DoubleType(), True),
        StructField("bedrooms", DoubleType(), True),
        StructField("livingArea", DoubleType(), True),
        StructField("isFeatured", DoubleType(), True),
        StructField("isShowcaseListing", DoubleType(), True),
        StructField("description", StringType(), True) # Để trích xuất NLP features
        # Các cột NLP sẽ được tạo trong Pipeline (nếu Pipeline bao gồm bước đó)
        # Lưu ý: Nếu bước tạo cột NLP nằm NGOÀI Pipeline (như trong sparkML.py cũ),
        # bạn phải tự tạo cột đó ở đây trước khi đưa vào model.transform
    ])
    
    # Ở file sparkML.py mới, tôi đã giả định các bước NLP nằm ngoài Pipeline cho đơn giản.
    # Nên ở đây ta cần tái tạo logic feature engineering cơ bản
    rows = [input_data]
    df_input = spark.createDataFrame(rows) # Schema tự suy diễn hoặc ép kiểu sau
    
    # Tái tạo logic NLP cơ bản (giống sparkML.py)
    from pyspark.sql.functions import lit, col, lower, when
    df_processed = df_input \
        .withColumn("desc_lower", lower(col("description"))) \
        .withColumn("has_red_book", when(col("desc_lower").rlike("sổ đỏ|sổ hồng"), 1.0).otherwise(0.0)) \
        .withColumn("is_street_front", when(col("desc_lower").rlike("mặt tiền|kinh doanh"), 1.0).otherwise(0.0)) \
        .withColumn("is_wide_alley", when(col("desc_lower").rlike("xe hơi|oto"), 1.0).otherwise(0.0)) \
        .withColumn("isFeatured", lit(0.0)) \
        .withColumn("isShowcaseListing", lit(0.0))
        
    # Ép kiểu cho khớp model
    numeric_cols = ['lotAreaValue', 'bathrooms', 'bedrooms', 'livingArea']
    for c in numeric_cols:
        df_processed = df_processed.withColumn(c, col(c).cast(DoubleType()))

    # Dự đoán
    prediction = model.transform(df_processed)
    return prediction.select("prediction").collect()[0][0]

# ==========================================
# 3. GIAO DIỆN STREAMLIT
# ==========================================
st.set_page_config(page_title="Real Estate AI Dashboard", layout="wide")

st.title("🏙️ Real Estate AI Analytics Center")
st.markdown("---")

# --- PHẦN 1: THỐNG KÊ TỔNG HỢP (READ CASSANDRA) ---
st.header("1. Thị trường Tổng quan")
if st.button("🔄 Cập nhật dữ liệu"):
    st.cache_data.clear()

stats_df = get_dashboard_stats()

if not stats_df.empty:
    col1, col2, col3 = st.columns(3)
    col1.metric("Tổng số tin đăng", f"{stats_df['count'].sum():,}")
    col1.metric("Giá trung bình toàn thị trường", f"{stats_df['avg_price'].mean()/1e9:,.2f} Tỷ")
    
    # Biểu đồ giá theo thành phố
    fig = px.bar(stats_df, x='city', y='avg_price', 
                 title="Giá trung bình theo Quận/Huyện",
                 color='count', labels={'avg_price': 'Giá TB (VNĐ)'})
    st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("Chưa có dữ liệu trong Cassandra.")

st.markdown("---")

# --- PHẦN 2: DỰ ĐOÁN GIÁ (INPUT FORM) ---
st.header("2. Định giá Nhà (AI Prediction)")

c1, c2, c3 = st.columns(3)
with c1:
    inp_city = st.selectbox("Quận/Huyện", ["Hà Đông", "Cầu Giấy", "Đống Đa", "Thanh Xuân", "Hoàng Mai"])
    inp_area = st.number_input("Diện tích (m2)", min_value=10.0, value=50.0)
with c2:
    inp_bed = st.number_input("Số phòng ngủ", 1, 10, 2)
    inp_bath = st.number_input("Số phòng tắm", 1, 10, 2)
with c3:
    inp_desc = st.text_area("Mô tả (Ví dụ: Nhà mặt tiền, có sổ đỏ)", "Nhà có sổ đỏ, ngõ xe hơi")

if st.button("🔮 Dự đoán giá ngay"):
    if model:
        with st.spinner("AI đang tính toán..."):
            input_data = {
                "city": inp_city,
                "homeType": "UNKNOWN", # Giá trị mặc định
                "newConstructionType": "UNKNOWN",
                "lotAreaValue": inp_area,
                "livingArea": inp_area,
                "bathrooms": inp_bath,
                "bedrooms": inp_bed,
                "description": inp_desc
            }
            pred_price = predict_custom_house(input_data)
            st.success(f"💰 Giá dự đoán: **{pred_price/1e9:,.2f} Tỷ VNĐ**")
    else:
        st.error("Chưa load được Model. Hãy chạy file sparkML.py để train trước!")

# --- PHẦN 3: XEM GỢI Ý (READ CASSANDRA RECOMMENDATIONS) ---
st.markdown("---")
st.header("3. Gợi ý Nhà Tương tự")

# Nhập ID căn nhà muốn xem (trong thực tế sẽ click từ danh sách)
target_id = st.text_input("Nhập mã căn nhà (ZPID) để xem gợi ý:", "")

if target_id:
    # Query bảng recommendations từ Cassandra
    try:
        rec_df_spark = spark.read \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="recommendations", keyspace="finaldata1") \
            .load() \
            .filter(f"source_id = '{target_id}'")
        
        recs = rec_df_spark.toPandas()
        
        if not recs.empty:
            st.write(f"Tìm thấy {len(recs)} căn tương tự:")
            st.dataframe(recs[['target_id', 'city', 'price', 'livingarea', 'distance']])
        else:
            st.info("Không tìm thấy gợi ý cho căn này (Có thể do chưa chạy Batch Job).")
            
    except Exception as e:
        st.error(f"Lỗi truy vấn gợi ý: {e}")