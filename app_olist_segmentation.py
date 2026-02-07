import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os

# 1. 페이지 설정 및 프리미엄 스타일링
st.set_page_config(page_title="Olist 구매자 가치-경험 매트릭스", layout="wide")

# 커스텀 CSS로 디자인 강화
st.markdown("""
    <style>
    .main { background-color: #f8f9fa; }
    .stMetric { background-color: #ffffff; padding: 15px; border-radius: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); }
    .insight-card { 
        padding: 20px; border-radius: 12px; margin-bottom: 20px; 
        border-left: 5px solid #1f77b4; background-color: #ffffff;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    h1, h2, h3 { color: #1e293b; font-family: 'Inter', sans-serif; }
    </style>
""", unsafe_allow_html=True)

# 2. 데이터 로드 로직 (Parquet 최적화 및 다중 경로 지원)
@st.cache_data
def load_data():
    current_dir = os.path.dirname(__file__)
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
        st.error("데이터 파일을 찾을 수 없습니다. 경로와 파일 전송 상태를 확인해주세요.")
        st.stop()
    
    # 데이터 로드
    orders = pd.read_parquet(os.path.join(base_path, 'proc_olist_orders_dataset.parquet'))
    items = pd.read_parquet(os.path.join(base_path, 'proc_olist_order_items_dataset.parquet'))
    reviews = pd.read_parquet(os.path.join(base_path, 'proc_olist_order_reviews_dataset.parquet'))
    customers = pd.read_parquet(os.path.join(base_path, 'proc_olist_customers_dataset.parquet'))
    products = pd.read_parquet(os.path.join(base_path, 'proc_olist_products_dataset.parquet'))
    
    # 시간 데이터 및 지연 일수 계산
    orders['order_delivered_customer_date'] = pd.to_datetime(orders['order_delivered_customer_date'])
    orders['order_estimated_delivery_date'] = pd.to_datetime(orders['order_estimated_delivery_date'])
    orders['delay_days'] = (orders['order_delivered_customer_date'] - orders['order_estimated_delivery_date']).dt.days
    orders['delay_days'] = orders['delay_days'].clip(lower=0)
    
    # 주문별 단가 합계 및 카테고리 정보
    order_items = items.merge(products[['product_id', 'product_category_name_english']], on='product_id', how='left')
    order_summary = order_items.groupby('order_id').agg({
        'price': 'sum',
        'product_category_name_english': lambda x: x.iloc[0] if not x.empty else 'Unknown'
    }).reset_index()
    
    # 통합 병합
    df = orders.merge(customers[['customer_id', 'customer_unique_id']], on='customer_id')
    df = df.merge(reviews[['order_id', 'review_score']], on='order_id')
    df = df.merge(order_summary, on='order_id')
    
    # 고객별 마스터 집계 (RFM + 경험 지표)
    cust_master = df.groupby('customer_unique_id').agg({
        'review_score': 'mean',
        'price': 'sum',
        'order_id': 'nunique',
        'delay_days': 'mean',
        'product_category_name_english': lambda x: x.value_counts().index[0]
    }).rename(columns={
        'review_score': 'Satisfaction',
        'price': 'Monetary',
        'order_id': 'Frequency',
        'delay_days': 'Avg_Delay',
        'product_category_name_english': 'Primary_Category'
    }).reset_index()
    
    # RFM 등급 부여 (구매액 기준 상위 10%, 30%, 나머지)
    m_thresholds = cust_master['Monetary'].quantile([0.7, 0.9]).values
    def rfm_grade(m):
        if m >= m_thresholds[1]: return 'VIP'
        elif m >= m_thresholds[0]: return 'Loyal'
        else: return 'Regular'
    cust_master['RFM_Grade'] = cust_master['Monetary'].apply(rfm_grade)
    
    return cust_master

try:
    df_cust = load_data()
except Exception as e:
    st.error(f"데이터 정합성 오류: {e}")
    st.stop()

# 3. 사이드바 컨트롤 (사용자화)
st.sidebar.header("🎯 전략적 필터링")
m_standard = st.sidebar.slider("매출 임계값 (Monetary)", 0, int(df_cust['Monetary'].quantile(0.95)), int(df_cust['Monetary'].median()))
s_standard = st.sidebar.slider("만족도 임계값 (Review Score)", 1.0, 5.0, 3.8, 0.1)

# 세그먼트 분류 로직 (통합)
def classify(row):
    if row['Monetary'] >= m_standard:
        return 'Premium Core' if row['Satisfaction'] >= s_standard else 'Critical Risk'
    else:
        return 'Potential Hero' if row['Satisfaction'] >= s_standard else 'Standard Starter'

df_cust['Segment'] = df_cust.apply(classify, axis=1)

# 4. 헤더 섹션
st.title("🛡️ Olist 구매자 통합 가치-경험 매트릭스 (Buyer Experience Matrix)")
st.markdown("단순한 매출액 이상으로, **물류 경험이 고객 가치에 미치는 영향**을 4분면 매트릭스로 분석합니다.")

# 지표 요약
m1, m2, m3, m4 = st.columns(4)
m1.metric("총 분석 구매자", f"{len(df_cust):,}")
m2.metric("평균 만족도", f"{df_cust['Satisfaction'].mean():.2f} ⭐")
m3.metric("평균 지연 일수", f"{df_cust['Avg_Delay'].mean():.1f} 일")
m4.metric("VIP 비중", f"{(df_cust['RFM_Grade']=='VIP').mean()*100:.1f}%")

st.divider()

# 5. 메인 시각화 (통합 매트릭스)
col_vis, col_desc = st.columns([2, 1])

with col_vis:
    st.subheader("📍 구매자 경험-가치 매트릭스")
    
    # 성능 샘플링 (고급 산점도)
    plot_df = df_cust.sample(min(len(df_cust), 5000), random_state=42)
    
    fig = px.scatter(
        plot_df,
        x='Satisfaction', y='Monetary',
        color='RFM_Grade', size='Avg_Delay',
        hover_name='customer_unique_id',
        hover_data=['Segment', 'Primary_Category', 'Frequency'],
        color_discrete_map={'VIP': '#1A3A5F', 'Loyal': '#3A7CA5', 'Regular': '#A2C4D8'},
        labels={'Satisfaction': '배송 만족도 (Review Score)', 'Monetary': '총 구매 가치 (Monetary)', 'RFM_Grade': '고객 등급'},
        height=650, template="plotly_white",
        size_max=30
    )
    
    # 4분면 영역 배경 및 텍스트 추가 (go 활용)
    fig.add_vline(x=s_standard, line_dash="dash", line_color="#cbd5e1")
    fig.add_hline(y=m_standard, line_dash="dash", line_color="#cbd5e1")
    
    # 영역 라벨링
    fig.add_annotation(x=4.5, y=plot_df['Monetary'].max()*0.9, text="<b>Premium Core</b>", showarrow=False, font=dict(size=14, color="#059669"))
    fig.add_annotation(x=1.5, y=plot_df['Monetary'].max()*0.9, text="<b>Critical Risk</b>", showarrow=False, font=dict(size=14, color="#dc2626"))
    fig.add_annotation(x=4.5, y=m_standard*0.3, text="<b>Potential Hero</b>", showarrow=False, font=dict(size=14, color="#2563eb"))
    fig.add_annotation(x=1.5, y=m_standard*0.3, text="<b>Standard Starter</b>", showarrow=False, font=dict(size=14, color="#64748b"))

    st.plotly_chart(fig, use_container_width=True)

with col_desc:
    st.subheader("🔍 세그먼트별 핵심 통찰")
    
    seg_stats = df_cust.groupby('Segment').agg({'Avg_Delay': 'mean', 'customer_unique_id': 'count'}).reset_index()
    
    for _, row in seg_stats.iterrows():
        color = "#059669" if row['Segment'] == 'Premium Core' else "#dc2626" if row['Segment'] == 'Critical Risk' else "#2563eb" if row['Segment'] == 'Potential Hero' else "#64748b"
        with st.container():
            st.markdown(f"""
                <div class='insight-card' style='border-left-color: {color};'>
                    <h4 style='margin:0;'>{row['Segment']}</h4>
                    <p style='color: gray; font-size: 0.9em;'>규모: {row['customer_unique_id']:,}명</p>
                    <p><b>평균 배송 지연:</b> {row['Avg_Delay']:.1f}일</p>
                </div>
            """, unsafe_allow_html=True)
    
    st.info("💡 **버블 크기 분석**: 원의 크기가 클수록 물류 성능이 저하되었음을 의미하며, Critical Risk 영역의 버블 밀집도는 서비스 이탈의 직접적 원인을 시연합니다.")

# 6. 페르소나 정의 및 전략 가이드 (개편)
st.divider()
st.subheader("🎭 Olist 구매자 페르소나 리포트: 경험 기반 성장 전략")

p1, p2 = st.columns(2)

with p1:
    st.markdown("""
    ### 🥇 [Premium Core] 혁신 성장의 동력
    - **핵심 지표:** 고매출 + 고만족 (안정적인 배송 만족도 유지)
    - **분석:** 이들은 주로 **'핵심 판매자(Core Sellers)'** 및 신뢰도 높은 물류망을 이용하는 우량 고객입니다.
    - **전략:** 이들의 기대치는 업계 최고 수준입니다. 지연 발생 시 즉각적인 보상과 'VVIP 전용 물류 라인' 확보를 통해 이탈 가능성을 0%로 유지해야 합니다.
    
    ### 🧨 [Critical Risk] 불안정 성장의 희생양
    - **핵심 지표:** 고매출 + 저만족 (높은 구매 가치에도 불구하고 지연 발생)
    - **분석:** 매출 규모는 크지만 운영 점수가 낮은 **'불안정 성장 판매자'**와 연결될 확률이 가장 높습니다. 가장 큰 자산 손실이 발생하는 구간입니다.
    - **전략:** 이 세그먼트의 발생 원인은 90%가 '물류 성능'에 있습니다. 판매자에게 강력한 패널티를 부여하거나, 플랫폼 차원의 '배송 약속 보장제'를 통해 신뢰를 회복해야 합니다.
    """)

with p2:
    st.markdown("""
    ### 🚀 [Potential Hero] 가성비 기반 잠재 충성군
    - **핵심 지표:** 저매출 + 고만족 (가벼운 구매 빈도와 높은 서비스 만족도)
    - **분석:** 구매 단가는 낮지만 긍정적인 경험을 축적 중인 단계입니다. 주로 생필품/액세서리 등의 카테고리를 이용합니다.
    - **전략:** '만족스러운 경험'을 '더 큰 구매'로 연결하는 전환 캠페인이 필요합니다. 무료 배송 임계값 설정을 통해 객단가를 높여 VIP로 유도하십시오.
    
    ### ⚠️ [Standard Starter] 초기 서비스의 가늠자
    - **핵심 지표:** 저매출 + 저만족 (낮은 상호작용 및 부정적 피드백)
    - **분석:** 주로 **'초기 진입 판매자'** 또는 물류 인프라가 취약한 원거리 지역(AL, MA 등)의 고객들입니다.
    - **전략:** 첫 구매 경험이 실패로 돌아간 그룹입니다. 이들에게는 재구매 유도보다는 '부정 리뷰의 확산 방지'가 급선무이며, 사은품 증정 등 감성적 품질 관리가 필요합니다.
    """)

# 7. 판매자 대조 인사이트 (보충 내용)
st.success("""
### 🎯 판매자-구매자 시너지 인사이트
**'불안정 성장 판매자'**의 매출 비중이 높아질수록, 매트릭스의 **Critical Risk(좌상단)** 영역이 급격히 팽창합니다. 
이는 플랫폼 전체의 LTV(고객 생애 가치)를 갉아먹는 행위입니다. 매출 증대 전략 시 반드시 해당 판매자의 
**'배송 지연 일수 및 고객 리뷰 연동'**을 모니터링하여 Critical Risk 고객을 Premium Core로 이동시키는 물류 효율화 작업이 선행되어야 합니다.
""")

st.caption("Olist Data Analysis Dashboard v2.0 | 통합 경험-가치 매트릭스 리포트")
