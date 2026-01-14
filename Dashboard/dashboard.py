import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import time  # <--- [THÊM] Thư viện để đếm giờ

# ==========================================
# 1. CẤU HÌNH TRANG & CSS
# ==========================================
st.set_page_config(
    page_title="Zillow Analytics Dashboard",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .big-font { font-size:30px !important; font-weight: bold; color: #1E88E5; }
    .metric-card { background-color: #f0f2f6; border-radius: 10px; padding: 15px; box-shadow: 2px 2px 5px rgba(0,0,0,0.1); }
    div[data-testid="stMetricValue"] { font-size: 24px; color: #000; }
</style>
""", unsafe_allow_html=True)

# ==========================================
# 2. HÀM LOAD DỮ LIỆU (QUAN TRỌNG NHẤT)
# ==========================================

# [SỬA 1] Thêm ttl=2: Cache chỉ sống 2 giây. 
# Sau 2 giây, nếu hàm được gọi lại, nó SẼ ĐỌC LẠI FILE TỪ Ổ CỨNG.
@st.cache_data(ttl=2) 
def load_prediction_data():
    try:
        df = pd.read_csv("D:/BTL_Big_Data/Big_Data_20251/prediction_results.csv")
        df['price_Ty'] = df['price'] / 1_000_000_000
        df['pred_Ty'] = df['prediction'] / 1_000_000_000
        df['Error'] = df['price_Ty'] - df['pred_Ty']
        df['Abs_Error'] = df['Error'].abs()
        return df
    except FileNotFoundError:
        return None

# [SỬA 1] Tương tự với file Recommendation
@st.cache_data(ttl=2)
def load_recommendation_data():
    try:
        df = pd.read_csv("D:/BTL_Big_Data/Big_Data_20251/recommendation_results.csv")
        return df
    except FileNotFoundError:
        return None

# ==========================================
# 3. SIDEBAR (BỘ LỌC)
# ==========================================
st.sidebar.image("spark-logo.jpg", width=150)
st.sidebar.title("🎛️ Control Panel")

# Hiển thị trạng thái Real-time
st.sidebar.success("🟢 Real-time Mode: ON")
st.sidebar.write("Đang tự động cập nhật mỗi 2s...")

df = load_prediction_data()

if df is not None:
    city_list = ["Tất cả"] + sorted(df['city'].unique().tolist())
    
    # Lưu session state để giữ lựa chọn khi refresh trang
    if 'selected_city' not in st.session_state:
        st.session_state.selected_city = "Tất cả"

    # Widget selectbox cần có key để giữ trạng thái
    selected_city = st.sidebar.selectbox(
        "📍 Chọn Khu vực (Quận/Huyện):", 
        city_list, 
        key='city_select_box'
    )
    
    if selected_city != "Tất cả":
        df_filtered = df[df['city'] == selected_city]
    else:
        df_filtered = df
else:
    st.error("Đang chờ dữ liệu từ Spark ML...")
    time.sleep(2)
    st.rerun()
    st.stop()

# ==========================================
# 4. MAIN LAYOUT
# ==========================================
st.markdown('<p class="big-font">🏠 HANOI REAL ESTATE AI ANALYTICS</p>', unsafe_allow_html=True)
st.markdown(f"**Hệ thống:** Spark Streaming + Batch Processing + GBT Regressor | **Khu vực:** {selected_city}")

tab1, tab2 = st.tabs(["📊 Phân tích & Dự báo", "🤖 Hệ thống Gợi ý (AI)"])

# --- TAB 1 ---
with tab1:
    col1, col2, col3, col4 = st.columns(4)
    with col1: st.metric("Tổng số căn tin", f"{len(df_filtered):,}", delta="Samples")
    with col2: 
        r2 = 1 - (df_filtered['Error']**2).sum() / ((df_filtered['price_Ty'] - df_filtered['price_Ty'].mean())**2).sum()
        st.metric("Độ chính xác (R2)", f"{r2:.4f}")
    with col3: 
        rmse = (df_filtered['Error']**2).mean() ** 0.5
        st.metric("RMSE", f"{rmse:.2f} Tỷ")
    with col4: 
        mae = df_filtered['Abs_Error'].mean()
        st.metric("MAE", f"{mae:.2f} Tỷ")

    st.markdown("---")
    
    col_chart1, col_chart2 = st.columns([1.5, 1])
    with col_chart1:
        fig = px.scatter(
            df_filtered, x="price_Ty", y="pred_Ty", color="Abs_Error",
            size="livingarea", hover_data=['city', 'bedrooms'],
            title=f"Độ chính xác dự báo", color_continuous_scale="RdYlGn_r"
        )
        max_val = max(df_filtered['price_Ty'].max(), df_filtered['pred_Ty'].max())
        fig.add_shape(type="line", x0=0, y0=0, x1=max_val, y1=max_val, line=dict(color="Red", width=2, dash="dash"))
        st.plotly_chart(fig, use_container_width=True)

    with col_chart2:
        fig2 = px.histogram(df_filtered, x="Error", nbins=30, title="Phân bố sai số", color_discrete_sequence=['#1E88E5'])
        fig2.add_vline(x=0, line_dash="dash", line_color="red")
        st.plotly_chart(fig2, use_container_width=True)

    st.subheader("📋 Dữ liệu chi tiết")
    st.dataframe(
        df_filtered.sort_values("Abs_Error").head(10)[['city', 'livingarea', 'bedrooms', 'price_Ty', 'pred_Ty', 'Error']],
        use_container_width=True
    )

# --- TAB 2 ---
with tab2:
    rec_df = load_recommendation_data()
    if rec_df is not None:
        target_house = rec_df.iloc[0]
        neighbors = rec_df.iloc[1:]
        
        st.info("🤖 AI Recommendation Engine đang chạy...")
        st.write("#### 🏠 Căn nhà bạn đang xem:")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Khu vực", target_house['city'])
        c2.metric("Mức giá", f"{target_house['price']/1e9:,.2f} Tỷ")
        c3.metric("Diện tích", f"{target_house['livingarea']} m2")
        c4.metric("Phòng ngủ", f"{int(target_house['bedrooms'])}")

        st.write("#### 🔍 Top 5 Căn nhà tương tự:")
        cols = st.columns(5)
        for idx, (_, row) in enumerate(neighbors.head(5).iterrows()):
            with cols[idx]:
                st.image("https://cdn-icons-png.flaticon.com/512/263/263115.png", width=60)
                st.markdown(f"**{row['city']}**")
                st.write(f"💰 {row['price']/1e9:,.2f} Tỷ")
                st.progress(int(row['Do_Giong_Nhau'])/100, text=f"Giống: {int(row['Do_Giong_Nhau'])}%")
    else:
        st.warning("Đang chờ dữ liệu gợi ý...")

# ==========================================
# [SỬA 2] CƠ CHẾ TỰ ĐỘNG REFRESH
# ==========================================
time.sleep(2) # Nghỉ 2 giây
st.rerun()    # Ra lệnh cho Streamlit chạy lại từ đầu