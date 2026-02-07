import streamlit as st
import pandas as pd
import plotly.express as px
import os

# 페이지 설정
st.set_page_config(page_title="Olist 구매자 심층 분석 대시보드", layout="wide")

# 데이터 로드 함수 (캐싱 사용)
@st.cache_data
def load_data():
    # 현재 파일의 디렉토리 경로
    current_dir = os.path.dirname(__file__)
    
    # 탐색 후보군: 1. 루트(개별 업로드), 2. DATA_PARQUET 폴더, 3. 로컬 절대 경로
    search_paths = [
        current_dir,
        os.path.join(current_dir, 'DATA_PARQUET'),
        r'c:\fcicb6\data\OLIST_V.2\DATA_PARQUET'
    ]
    
    base_path = None
    target_check_file = 'proc_olist_orders_dataset.parquet'
    
    for p in search_paths:
        if os.path.exists(os.path.join(p, target_check_file)):
            base_path = p
            break
            
    if not base_path:
        st.error("데이터 파일을 찾을 수 없습니다. 모든 .parquet 파일이 앱 파일과 같은 위치에 있거나 'DATA_PARQUET' 폴더 안에 있는지 확인해주세요.")
        st.stop()
    
    # 데이터 읽기 (Parquet 포맷)
    orders = pd.read_parquet(os.path.join(base_path, 'proc_olist_orders_dataset.parquet'))
    items = pd.read_parquet(os.path.join(base_path, 'proc_olist_order_items_dataset.parquet'))
    reviews = pd.read_parquet(os.path.join(base_path, 'proc_olist_order_reviews_dataset.parquet'))
    customers = pd.read_parquet(os.path.join(base_path, 'proc_olist_customers_dataset.parquet'))
    products = pd.read_parquet(os.path.join(base_path, 'proc_olist_products_dataset.parquet'))
    
    # 전처리: 배송 지연 계산
    orders['order_delivered_customer_date'] = pd.to_datetime(orders['order_delivered_customer_date'])
    orders['order_estimated_delivery_date'] = pd.to_datetime(orders['order_estimated_delivery_date'])
    orders['delay_days'] = (orders['order_delivered_customer_date'] - orders['order_estimated_delivery_date']).dt.days
    orders['delay_days'] = orders['delay_days'].apply(lambda x: x if x > 0 else 0)

    # 1. 주문별 평균 리뷰 점수
    order_reviews = reviews.groupby('order_id')['review_score'].mean().reset_index()
    
    # 2. 주문-고객 맵핑
    order_cust = orders.merge(customers[['customer_id', 'customer_unique_id']], on='customer_id', how='inner')
    
    # 3. 주문 상세 (가격 + 카테고리)
    items_with_cats = items.merge(products[['product_id', 'product_category_name_english']], on='product_id', how='left')
    
    # 고객별 집계
    # 1. 리뷰/만족도 및 지연 발생
    cust_review_delay = order_cust.merge(order_reviews, on='order_id', how='inner').groupby('customer_unique_id').agg({
        'review_score': 'mean',
        'delay_days': 'mean'
    }).reset_index()
    
    # 2. 구매액 및 빈도
    order_summary = items.groupby('order_id')['price'].sum().reset_index()
    cust_monetary = order_cust.merge(order_summary, on='order_id', how='inner').groupby('customer_unique_id').agg({
        'price': 'sum',
        'order_id': 'nunique'
    }).reset_index().rename(columns={'price': 'Total_Monetary', 'order_id': 'Frequency'})
    
    # 3. 최종 집계
    cust_agg = cust_review_delay.merge(cust_monetary, on='customer_unique_id', how='inner').rename(columns={'review_score': 'Avg_Satisfaction'})
    
    # RFM 등급 (간이)
    m_bins = [0, cust_agg['Total_Monetary'].quantile(0.5), cust_agg['Total_Monetary'].quantile(0.8), float('inf')]
    cust_agg['RFM_Segment'] = pd.cut(cust_agg['Total_Monetary'], bins=m_bins, labels=['Regular', 'Loyal', 'VIP'])

    # 카테고리 정보
    cust_cat_map = order_cust.merge(items_with_cats[['order_id', 'product_category_name_english']], on='order_id', how='inner')
    cust_cat_map = cust_cat_map[['customer_unique_id', 'product_category_name_english']]
    
    return cust_agg, cust_cat_map

# 데이터 로드
try:
    df, cust_cat_map = load_data()
except Exception as e:
    st.error(f"데이터를 불러오는 중 오류가 발생했습니다: {e}")
    st.stop()

# 타이틀
st.title("🛍️ Olist 구매자 심층 분석 대시보드")
st.markdown("구매자의 행동 패턴과 만족도를 다각도로 분석하여 최적화된 마케팅 인사이트를 도출합니다.")

# 탭 구성
tab_seg, tab_matrix = st.tabs(["📊 구매자 4대 유형 분류", "📈 경험 가치 vs 물류 성능"])

# --- TAB 1: 구매자 4대 유형 분류 ---
with tab_seg:
    st.subheader("📌 만족도와 구매액 기준 세그먼트")
    
    # 사이드바 설정 (공통 활용을 위해 탭 내부에서 호출 가능하지만 여기선 구분)
    col1, col2 = st.columns([2, 1])
    
    with col1:
        m_threshold = st.slider("매출 임계값 (Monetary)", 0, int(df['Total_Monetary'].quantile(0.95)), int(df['Total_Monetary'].median()), key="s1")
        sat_threshold = st.slider("만족도 임계값 (Satisfaction)", 1.0, 5.0, 3.5, 0.1, key="s2")
        
        def assign_segment_1(row):
            if row['Total_Monetary'] >= m_threshold and row['Avg_Satisfaction'] >= sat_threshold: return '우상단 (VIP)'
            elif row['Total_Monetary'] >= m_threshold and row['Avg_Satisfaction'] < sat_threshold: return '좌상단 (위험 고객)'
            elif row['Total_Monetary'] < m_threshold and row['Avg_Satisfaction'] >= sat_threshold: return '우하단 (잠재 충성군)'
            else: return '좌하단 (이탈 우려)'

        df['Segment_Type'] = df.apply(assign_segment_1, axis=1)
        
        plot_df = df.sample(min(len(df), 5000), random_state=42)
        fig1 = px.scatter(
            plot_df, x='Avg_Satisfaction', y='Total_Monetary', size='Frequency', color='Segment_Type',
            hover_name='customer_unique_id', height=500,
            color_discrete_map={'우상단 (VIP)': '#00CC96', '좌상단 (위험 고객)': '#EF553B', '우하단 (잠재 충성군)':'#636EFA', '좌하단 (이탈 우려)': '#AB63FA'}
        )
        fig1.add_vline(x=sat_threshold, line_dash="dash", line_color="gray")
        fig1.add_hline(y=m_threshold, line_dash="dash", line_color="gray")
        st.plotly_chart(fig1, use_container_width=True)

    with col2:
        st.markdown("### 💡 유형별 주요 카테고리")
        for seg in ['우상단 (VIP)', '좌상단 (위험 고객)', '우하단 (잠재 충성군)', '좌하단 (이탈 우려)']:
            seg_custs = df[df['Segment_Type'] == seg]['customer_unique_id']
            top_cats = cust_cat_map[cust_cat_map['customer_unique_id'].isin(seg_custs)]['product_category_name_english'].value_counts().head(3).index.tolist()
            st.markdown(f"**{seg}**")
            st.write(", ".join(top_cats) if top_cats else "추출 불가")
            st.divider()

# --- TAB 2: 경험 가치 vs 물류 성능 ---
with tab_matrix:
    st.subheader("📌 물류 지연과 RFM 등급 기반 심층 분석")
    st.markdown("**점의 크기**가 클수록 배송 지연 일수가 길다는 것을 의미합니다.")
    
    c1, c2 = st.columns([3, 1])
    
    with c1:
        m_threshold_2 = st.slider("매출 기준점 (Monetary)", 0, int(df['Total_Monetary'].quantile(0.95)), int(df['Total_Monetary'].median()), key="m1")
        sat_threshold_2 = st.slider("만족도 기준점 (Satisfaction)", 1.0, 5.0, 3.5, 0.1, key="m2")
        
        def assign_segment_2(row):
            if row['Total_Monetary'] >= m_threshold_2 and row['Avg_Satisfaction'] >= sat_threshold_2: return '핵심 구매자 (Core Buyers)'
            elif row['Total_Monetary'] >= m_threshold_2 and row['Avg_Satisfaction'] < sat_threshold_2: return '불만 고액 고객 (Upset High-spenders)'
            elif row['Total_Monetary'] < m_threshold_2 and row['Avg_Satisfaction'] >= sat_threshold_2: return '실속 만족 고객 (Efficient Buyers)'
            else: return '이탈 우려 고객 (At-risk Starters)'

        df['Quadrant'] = df.apply(assign_segment_2, axis=1)
        
        plot_df_2 = df.sample(min(len(df), 5000), random_state=42)
        fig2 = px.scatter(
            plot_df_2, x='Avg_Satisfaction', y='Total_Monetary', size='delay_days', color='RFM_Segment',
            hover_name='customer_unique_id', hover_data=['Quadrant', 'delay_days'],
            color_discrete_map={'VIP': '#FFD700', 'Loyal': '#636EFA', 'Regular': '#AB63FA'}, height=600,
            labels={'Avg_Satisfaction': '평균 배송 만족도', 'Total_Monetary': '총 구매 금액', 'delay_days': '평균 지연 일수'}
        )
        fig2.add_vline(x=sat_threshold_2, line_dash="dash", line_color="gray")
        fig2.add_hline(y=m_threshold_2, line_dash="dash", line_color="gray")
        st.plotly_chart(fig2, use_container_width=True)

    with c2:
        st.markdown("### 🔍 주요 분석 포인트")
        q_counts = df['Quadrant'].value_counts()
        for q in ['핵심 구매자 (Core Buyers)', '불만 고액 고객 (Upset High-spenders)', '실속 만족 고객 (Efficient Buyers)', '이탈 우려 고객 (At-risk Starters)']:
            st.metric(q.split('(')[0], f"{q_counts.get(q, 0):,}명")
        
        st.info("🎯 **대조 인사이트**\n'불안정 성장 판매자'는 주로 **불만 고액 고객** 세그먼트 형성에 영향을 미치며, 이는 고액 자산가들의 이탈을 초래합니다.")

# --- 하단 상세 설명 ---
st.divider()
st.subheader("📖 분석 가이드 및 페르소나 정의")
g_col1, g_col2 = st.columns(2)

with g_col1:
    st.markdown("""
    #### 1. VIP 파워 쇼퍼 (Core Buyers)
    - **분석:** Olist의 핵심 자산입니다. 안정적인 배송 서비스를 경험 중입니다.
    - **가이드:** 기대치가 매우 높으므로 사소한 지연도 치명적입니다. 프리미엄 포장을 권장합니다.
    
    #### 2. 고가치 이탈 위험군 (Upset High-spenders)
    - **분석:** 배송 지연으로 화가 난 고액 결제자입니다. **불안정 성장 판매자**의 제품을 샀을 가능성이 높습니다.
    - **가이드:** 배송 예정일을 보수적으로 설정하고 선제적인 CS 대응이 필수입니다.
    """)

with g_col2:
    st.markdown("""
    #### 3. 가성비 중시형 (Efficient Buyers)
    - **분석:** 생필품 등을 구매하며 물류 회전율에 만족하는 실속형 그룹입니다.
    - **가이드:** 배송비에 매우 민감하므로 배송비를 포함한 가격 노출 전략이 유효합니다.
    
    #### 4. 저가치 불만족군 (At-risk Starters)
    - **분석:** 초기 단계 판매자나 물류 취약 지역 고객이 다수 포함됩니다.
    - **가이드:** 판매 초기에는 이들의 부정 리뷰가 치명적이므로 안정적인 지역 위주로 판매를 시작하세요.
    """)

st.caption("Olist Data Analysis Dashboard | Generated by Antigravity AI")
