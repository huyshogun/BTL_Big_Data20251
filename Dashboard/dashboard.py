import streamlit as st
import pandas as pd
import plotly.express as px
import os
import sys

# ==========================================
# 0. CẤU HÌNH MÔI TRƯỜNG "XE TĂNG"
# ==========================================
os.environ['HADOOP_HOME'] = "D:\\hadoop"
sys.path.append("D:\\hadoop\\bin")
os.environ['JAVA_HOME'] = "C:\\Program Files\\Eclipse Adoptium\\jdk-11.0.29.7-hotspot"
os.environ['PATH'] = os.environ['JAVA_HOME'] + "\\bin;" + os.environ['PATH']

# Cấu hình mạng để Spark không bị lạc lối trên Windows
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import lit, col, lower, when

# ==========================================
# 1. SETUP SPARK SESSION
# ==========================================
@st.cache_resource
def get_spark_session():
    return SparkSession.builder \
    .appName("HanoiHousePrice_Dashboard") \
    .master("local[1]") \
    \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    \
    .config("spark.network.timeout", "600s") \
    .config("spark.executor.heartbeatInterval", "120s") \
    \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "4") \
    \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
    \
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.3.0") \
    .config("spark.cassandra.connection.host", "localhost") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

@st.cache_resource
def load_trained_model(_spark):
    try:
        model_path = "file:///D:/BTL_Big_Data/Big_Data_20251/saved_models"
        return PipelineModel.load(model_path)
    except Exception as e:
        st.error(f"❌ Không thể load Model: {e}")
        return None

spark = get_spark_session()
model = load_trained_model(spark)

# ==========================================
# 2. HELPER FUNCTIONS
# ==========================================
def get_market_stats():
    """Lấy thống kê thị trường từ bảng data2"""
    try:
        df = spark.read \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="data2", keyspace="finaldata1") \
            .load()
        df.createOrReplaceTempView("market_data")
        
        return spark.sql("""
            SELECT city, COUNT(*) as count, AVG(price) as avg_price 
            FROM market_data WHERE city IS NOT NULL GROUP BY city
        """).toPandas()
    except:
        return pd.DataFrame()

def predict_price(input_dict):
    """Dự đoán giá cho căn nhà nhập tay"""
    if not model: return 0.0
    
    # Tạo DataFrame từ input
    df = spark.createDataFrame([input_dict])
    
    # Feature Engineering (Tái tạo logic lúc train)
    if "description" not in df.columns: df = df.withColumn("description", lit(""))
    
    df = df.withColumn("desc_lower", lower(col("description"))) \
           .withColumn("has_red_book", when(col("desc_lower").rlike("sổ đỏ|sổ hồng|chính chủ"), 1.0).otherwise(0.0)) \
           .withColumn("is_street_front", when(col("desc_lower").rlike("mặt tiền|mặt phố|kinh doanh"), 1.0).otherwise(0.0)) \
           .withColumn("is_wide_alley", when(col("desc_lower").rlike("xe hơi|ô tô|oto"), 1.0).otherwise(0.0))
    
    for c in ['city', 'homeType', 'newConstructionType']:
        if c not in df.columns: df = df.withColumn(c, lit("UNKNOWN"))
        
    for c in ['isFeatured', 'isShowcaseListing']:
        if c not in df.columns: df = df.withColumn(c, lit(0.0))
        
    cols_to_cast = ['lotAreaValue', 'bathrooms', 'bedrooms', 'livingArea', 
                    'has_red_book', 'is_street_front', 'is_wide_alley', 
                    'isFeatured', 'isShowcaseListing']
    for c in cols_to_cast:
        df = df.withColumn(c, col(c).cast(DoubleType()))
        
    return model.transform(df).select("prediction").collect()[0][0]

# ==========================================
# 3. GIAO DIỆN CHÍNH
# ==========================================
st.set_page_config(page_title="Real Estate AI Hub", layout="wide", page_icon="🏠")

st.title("🏠 Real Estate AI Analytics Center")
st.markdown("#### *Hệ thống Phân tích & Định giá Bất động sản Thông minh*")
st.markdown("---")

# --- TAB NAVIGATION ---
tab1, tab2, tab3 = st.tabs(["📊 Thị trường", "🔮 Định giá AI", "🔍 Tìm nhà (Smart Search)"])

# ----------------------------------------------------
# TAB 1: THỊ TRƯỜNG
# ----------------------------------------------------
with tab1:
    st.header("Tổng quan Thị trường Hà Nội")
    if st.button("🔄 Refresh Data", key="btn_refresh"):
        st.cache_data.clear()
        
    stats = get_market_stats()
    if not stats.empty:
        c1, c2 = st.columns(2)
        c1.metric("Tổng tin đăng", f"{stats['count'].sum():,}")
        c2.metric("Giá TB toàn thị trường", f"{stats['avg_price'].mean()/1e9:,.2f} Tỷ")
        
        fig = px.bar(stats, x='city', y='avg_price', color='count', 
                     title="Mặt bằng giá theo Quận", labels={'avg_price': 'Giá TB (VNĐ)'})
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Đang chờ dữ liệu từ hệ thống Big Data...")

# ----------------------------------------------------
# TAB 2: ĐỊNH GIÁ (PREDICTION)
# ----------------------------------------------------
with tab2:
    st.header("Công cụ Định giá Nhà AI")
    st.caption("Nhập thông tin căn nhà để AI ước tính giá trị thực")
    
    col1, col2 = st.columns(2)
    with col1:
        p_city = st.selectbox("Quận/Huyện", ["Hà Đông", "Cầu Giấy", "Đống Đa", "Thanh Xuân", "Hoàng Mai", "Hai Bà Trưng", "Tây Hồ"])
        p_area = st.number_input("Diện tích (m2)", 10.0, 500.0, 50.0)
        p_desc = st.text_area("Mô tả đặc điểm", "Nhà chính chủ, có sổ đỏ, ngõ ô tô đỗ cửa")
    with col2:
        p_bed = st.number_input("Phòng ngủ", 1, 10, 2)
        p_bath = st.number_input("Phòng tắm", 1, 10, 2)
        st.markdown("<br>", unsafe_allow_html=True) # Spacer
        if st.button("🚀 Định giá ngay", type="primary"):
            with st.spinner("AI đang phân tích hàng triệu dữ liệu..."):
                val = predict_price({
                    "city": p_city, "lotAreaValue": p_area, "livingArea": p_area,
                    "bedrooms": p_bed, "bathrooms": p_bath, "description": p_desc
                })
                st.success(f"💎 AI định giá căn nhà này khoảng: **{val/1e9:,.2f} Tỷ VNĐ**")

# ----------------------------------------------------
# TAB 3: TÌM NHÀ (SMART SEARCH) - ĐÃ CẬP NHẬT
# ----------------------------------------------------
with tab3:
    st.header("Tìm kiếm Nhà theo Nhu cầu")
    st.caption("Tìm các căn nhà thực tế đang bán khớp với túi tiền của bạn")
    
    # 1. Input Filter
    c1, c2, c3 = st.columns(3)
    with c1:
        budget = st.number_input("Ngân sách của bạn (Tỷ VNĐ)", 1.0, 50.0, 3.0, step=0.5)
    with c2:
        target_cities = st.multiselect("Khu vực mong muốn", 
                                       ["Hà Đông", "Cầu Giấy", "Đống Đa", "Thanh Xuân", "Hoàng Mai", "Tây Hồ"],
                                       default=["Hà Đông", "Thanh Xuân"])
    with c3:
        min_bed = st.slider("Số phòng ngủ tối thiểu", 1, 5, 2)

    # 2. Logic Tìm kiếm
    if st.button("🔎 Tìm nhà phù hợp"):
        try:
            # Load dữ liệu gốc từ Cassandra (Table data2)
            df_search = spark.read \
                .format("org.apache.spark.sql.cassandra") \
                .options(table="data2", keyspace="finaldata1") \
                .load()
            
            df_search.createOrReplaceTempView("all_houses")
            
            # Logic: Tìm nhà giá chênh lệch +/- 20% so với ngân sách
            min_p = (budget * 1e9) * 0.8
            max_p = (budget * 1e9) * 1.2
            
            # Xây dựng câu SQL linh động
            city_condition = ""
            if target_cities:
                cities_str = "', '".join(target_cities)
                city_condition = f"AND city IN ('{cities_str}')"
            
            # Query (Lưu ý: Tên cột trong Cassandra thường là chữ thường)
            query = f"""
                SELECT zpid, city, price, livingarea, bedrooms, bathrooms 
                FROM all_houses 
                WHERE price >= {min_p} 
                  AND price <= {max_p} 
                  AND bedrooms >= {min_bed}
                  {city_condition}
                LIMIT 20
            """
            
            results = spark.sql(query).toPandas()
            
            # 3. Hiển thị kết quả
            if not results.empty:
                st.success(f"🎉 Tìm thấy {len(results)} căn nhà phù hợp với ngân sách ~{budget} Tỷ!")
                
                # Format lại hiển thị cho đẹp
                results['Giá (Tỷ)'] = results['price'].apply(lambda x: f"{x/1e9:.2f}")
                results['Diện tích'] = results['livingarea'].apply(lambda x: f"{x:.0f} m²")
                
                # Show bảng (Ẩn các cột thô đi)
                st.dataframe(
                    results[['city', 'Giá (Tỷ)', 'Diện tích', 'bedrooms', 'bathrooms', 'zpid']],
                    use_container_width=True,
                    hide_index=True
                )
            else:
                st.warning(f"Không tìm thấy căn nào quanh mức giá {budget} Tỷ ở khu vực này. Hãy thử nới rộng ngân sách!")
                
        except Exception as e:
            st.error(f"Lỗi truy vấn dữ liệu: {e}")
            st.info("Mẹo: Kiểm tra lại xem bảng 'data2' trong Cassandra đã có dữ liệu chưa.")