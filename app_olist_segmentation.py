import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os

# 페이지 설정
st.set_page_config(page_title="Olist 구매자 4대 유형 분석", layout="wide")

# 데이터 로드 함수 (캐싱 사용)
@st.cache_data
def load_data():
    # 데이터 경로 설정 - Parquet 폴더 사용 (배포 및 성능 최적화)
    # 현재 실행 중인 파일의 위치를 기준으로 경로 설정 (로컬/배포 호환)
    current_dir = os.path.dirname(__file__)
    base_path = os.path.join(current_dir, 'DATA_PARQUET')
    
    # 만약 위 경로에 데이터가 없으면 절대 경로 시도
    if not os.path.exists(base_path):
        base_path = r'c:\fcicb6\data\OLIST_V.2\DATA_PARQUET'
    
    # 필수 데이터 읽기 (Parquet 포맷)
    orders = pd.read_parquet(os.path.join(base_path, 'proc_olist_orders_dataset.parquet'))
    items = pd.read_parquet(os.path.join(base_path, 'proc_olist_order_items_dataset.parquet'))
    reviews = pd.read_parquet(os.path.join(base_path, 'proc_olist_order_reviews_dataset.parquet'))
    customers = pd.read_parquet(os.path.join(base_path, 'proc_olist_customers_dataset.parquet'))
    products = pd.read_parquet(os.path.join(base_path, 'proc_olist_products_dataset.parquet'))
    
    # 주문별 평균 리뷰 점수
    order_reviews = reviews.groupby('order_id')['review_score'].mean().reset_index()
    
    # 주문-고객 맵핑
    order_cust = orders.merge(customers[['customer_id', 'customer_unique_id']], on='customer_id', how='inner')
    
    # 주문 상세 (가격 + 카테고리)
    items_with_cats = items.merge(products[['product_id', 'product_category_name_english']], on='product_id', how='left')
    
    # 고객별 기초 통계 (Satisfaction, Monetary, Frequency)
    # 1. 고객별 리뷰 점수 평균
    cust_review = order_cust.merge(order_reviews, on='order_id', how='inner').groupby('customer_unique_id')['review_score'].mean().reset_index()
    
    # 2. 고객별 총 구매액 및 빈도
    order_summary = items.groupby('order_id')['price'].sum().reset_index()
    cust_monetary = order_cust.merge(order_summary, on='order_id', how='inner').groupby('customer_unique_id').agg({
        'price': 'sum',
        'order_id': 'nunique'
    }).reset_index().rename(columns={'price': 'Total_Monetary', 'order_id': 'Frequency'})
    
    # 3. 최종 집계 데이터프레임
    cust_agg = cust_review.merge(cust_monetary, on='customer_unique_id', how='inner').rename(columns={'review_score': 'Avg_Satisfaction'})
    
    # 4. 카테고리 정보 보관 (Top 3용)
    cust_cat_map = order_cust.merge(items_with_cats[['order_id', 'product_category_name_english']], on='order_id', how='inner')
    cust_cat_map = cust_cat_map[['customer_unique_id', 'product_category_name_english']]
    
    return cust_agg, cust_cat_map

# 데이터 로드
try:
    df, cust_cat_map = load_data()
except Exception as e:
    st.error(f"데이터를 불러오는 중 오류가 발생했습니다: {e}")
    st.stop()

# 타이틀 및 소개
st.title("📊 Olist 구매자 4대 유형 분류 및 시각화")
st.markdown("""
구매자의 **평균 리뷰 점수(Satisfaction)**와 **총 구매 금액(Monetary)**을 기준으로 고객을 4가지 유형으로 분류하고, 
각 유형별 특성과 주요 구매 카테고리를 분석합니다.
""")

# --- 사이드바: 임계값 설정 ---
st.sidebar.header("🕹️ 세그먼트 임계값 설정")
m_median = df['Total_Monetary'].median()
s_threshold = 3.5

m_threshold = st.sidebar.slider("금액 임계값 (Monetary)", 
                                min_value=0, 
                                max_value=int(df['Total_Monetary'].quantile(0.95)), 
                                value=int(m_median),
                                step=10)

sat_threshold = st.sidebar.slider("만족도 임계값 (Satisfaction)", 
                                  min_value=1.0, 
                                  max_value=5.0, 
                                  value=s_threshold,
                                  step=0.1)

# --- 세그먼트 분류 ---
def assign_segment(row):
    if row['Total_Monetary'] >= m_threshold and row['Avg_Satisfaction'] >= sat_threshold:
        return '우상단 (VIP)'
    elif row['Total_Monetary'] >= m_threshold and row['Avg_Satisfaction'] < sat_threshold:
        return '좌상단 (위험 고객)'
    elif row['Total_Monetary'] < m_threshold and row['Avg_Satisfaction'] >= sat_threshold:
        return '우하단 (잠재 충성군)'
    else:
        return '좌하단 (이탈 우려)'

df['Segment'] = df.apply(assign_segment, axis=1)

# --- 메인 레이아웃: 차트와 인사이트 ---
col_chart, col_insight = st.columns([2, 1])

with col_chart:
    st.subheader("📌 구매자 세그먼트 산점도")
    
    # Plotly 시각화
    # 데이터가 너무 많으면 로딩이 느릴 수 있으므로 샘플링
    plot_df = df.copy()
    if len(plot_df) > 5000:
        plot_df = plot_df.sample(5000, random_state=42)
        st.caption("ℹ️ 시각화 성능을 위해 5,000명의 데이터를 샘플링하여 표시합니다.")

    fig = px.scatter(
        plot_df,
        x='Avg_Satisfaction',
        y='Total_Monetary',
        size='Frequency',
        color='Segment',
        hover_name='customer_unique_id',
        hover_data={'Avg_Satisfaction': ':.2f', 'Total_Monetary': ':,.0f', 'Frequency': True, 'Segment': False},
        color_discrete_map={
            '우상단 (VIP)': '#00CC96',
            '좌상단 (위험 고객)': '#EF553B',
            '우하단 (잠재 충성군)':'#636EFA',
            '좌하단 (이탈 우려)': '#AB63FA'
        },
        labels={
            'Avg_Satisfaction': '평균 리뷰 점수 (Satisfaction)',
            'Total_Monetary': '총 구매 금액 (Monetary Value)',
            'Frequency': '주문 건수'
        },
        height=600,
        category_orders={"Segment": ['우상단 (VIP)', '좌상단 (위험 고객)', '우하단 (잠재 충성군)', '좌하단 (이탈 우려)']}
    )
    
    # 구분선 추가
    fig.add_vline(x=sat_threshold, line_dash="dash", line_color="gray", opacity=0.7)
    fig.add_hline(y=m_threshold, line_dash="dash", line_color="gray", opacity=0.7)
    
    st.plotly_chart(fig, use_container_width=True)

with col_insight:
    st.subheader("💡 유형별 주요 카테고리")
    
    segments = ['우상단 (VIP)', '좌상단 (위험 고객)', '우하단 (잠재 충성군)', '좌하단 (이탈 우려)']
    
    for seg in segments:
        seg_custs = df[df['Segment'] == seg]['customer_unique_id']
        seg_cats = cust_cat_map[cust_cat_map['customer_unique_id'].isin(seg_custs)]['product_category_name_english']
        top_cats = seg_cats.value_counts().head(3).index.tolist()
        
        st.markdown(f"### {seg}")
        if top_cats:
            for i, cat in enumerate(top_cats):
                st.write(f"{i+1}. {cat}")
        else:
            st.write("데이터 부족")
        st.divider()

# --- 하단 상세 분석 및 가이드 ---
st.divider()
st.subheader("📝 구매자 유형별 상세 특성 및 전략 가이드")

col1, col2 = st.columns(2)

with col1:
    st.info("""
    **1. 고가치 충성 고객 (VVIP & Loyal Buyers)**
    *   **특징:** 구매 금액이 매우 높고, 주로 가전/가구 등 고가 카테고리를 이용합니다.
    *   **분석 포인트:** 한 번의 배송 지연이나 품질 이슈에도 크게 실망할 수 있는 층입니다.
    *   **관리 전략:** '프리미엄 배송'과 '선제적 케어'를 통해 이탈을 방지해야 합니다.
    """)
    
    st.success("""
    **2. 실속형 다회 구매자 (Smart & Frequent Buyers)**
    *   **특징:** 만족도는 높지만 아직 건당 단가가 낮은 신규 또는 생필품 구매자입니다.
    *   **분석 포인트:** '무료 배송'이나 '쿠폰'에 반응도가 높습니다.
    *   **관리 전략:** 연관 상품 추천(Cross-selling)을 통해 구매 단가를 높이는 전략이 유효합니다.
    """)

with col2:
    st.error("""
    **3. 원거리 고위험 구매자 (High-Risk/Remote Buyers)**
    *   **특징:** 구매액은 크지만 배송 지연 등으로 인해 만족도가 낮은 상태입니다.
    *   **분석 포인트:** 주로 물류가 취약한 지역(AL, MA 등)에 거주할 확률이 높습니다.
    *   **관리 전략:** CS 전담 인력을 통한 사후 보상 및 배송 프로세스 개선이 시급합니다.
    """)
    
    st.warning("""
    **4. 신규/단발성 탐색 구매자 (New/One-time Explorers)**
    *   **특징:** 구매액과 만족도 모두 낮은 초기 단계 혹은 단순 호기심 고객입니다.
    *   **분석 포인트:** 서비스 경험에 따라 '잠재 충성군'이 될지 '이탈'할지 결정됩니다.
    *   **관리 전략:** 사은품, 손편지 등 '매력적 품질'을 시도하여 긍정적인 첫인상을 남겨야 합니다.
    """)

st.markdown("---")
st.caption("Olist Data Analysis Dashboard | Generated by Antigravity AI")
