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
        if m >= m_thresholds[1]: return 'VIP 고객'
        elif m >= m_thresholds[0]: return '충성 고객'
        else: return '일반 고객'
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
        return '핵심 우량 고객' if row['Satisfaction'] >= s_standard else '중점 관리(이탈 위험)'
    else:
        return '성장 잠재 고객' if row['Satisfaction'] >= s_standard else '일반 유입 고객'

df_cust['Segment'] = df_cust.apply(classify, axis=1)

# 4. 헤더 섹션
st.title("🛡️ Olist 구매자 통합 가치-경험 매트릭스")
st.markdown("구매 금액(가치)과 배송 만족도(경험)를 결합하여 고객의 상태를 다각도로 분석합니다.")

# 지표 요약
m1, m2, m3, m4 = st.columns(4)
m1.metric("총 분석 구매자", f"{len(df_cust):,}")
m2.metric("평균 만족도", f"{df_cust['Satisfaction'].mean():.2f} ⭐")
m3.metric("평균 지연 일수", f"{df_cust['Avg_Delay'].mean():.1f} 일")
m4.metric("VIP 비중", f"{(df_cust['RFM_Grade']=='VIP 고객').mean()*100:.1f}%")

st.divider()

# 5. 메인 시각화 (통합 매트릭스)
col_vis, col_desc = st.columns([2, 1])

with col_vis:
    st.subheader("📍 고객 경험-가치 매트릭스 시각화")
    
    # 성능 샘플링 (고급 산점도)
    plot_df = df_cust.sample(min(len(df_cust), 5000), random_state=42).copy()
    plot_df['Avg_Delay_Plot'] = plot_df['Avg_Delay'].fillna(0).clip(lower=0.1)
    
    fig = px.scatter(
        plot_df,
        x='Satisfaction', y='Monetary',
        color='RFM_Grade', size='Avg_Delay_Plot',
        hover_name='customer_unique_id',
        hover_data={'Segment': True, 'Primary_Category': True, 'Frequency': True, 'Avg_Delay': ':.1f', 'Avg_Delay_Plot': False},
        color_discrete_map={'VIP 고객': '#1A3A5F', '충성 고객': '#3A7CA5', '일반 고객': '#A2C4D8'},
        labels={
            'Satisfaction': '배송 만족도 (Review Score)', 
            'Monetary': '총 구매 가치 (Monetary)', 
            'RFM_Grade': '고객 등급',
            'Avg_Delay': '평균 지연 일수'
        },
        height=700, template="plotly_white",
        size_max=35
    )
    
    # 레이아웃 폰트 크기 조절
    fig.update_layout(
        font=dict(size=14),
        xaxis_title=dict(font=dict(size=16, color="black")),
        yaxis_title=dict(font=dict(size=16, color="black")),
        legend_title=dict(font=dict(size=14)),
        hoverlabel=dict(font_size=14)
    )
    
    # 4분면 영역 가이드선
    fig.add_vline(x=s_standard, line_dash="dash", line_color="#cbd5e1")
    fig.add_hline(y=m_standard, line_dash="dash", line_color="#cbd5e1")
    
    # 영역 라벨링 (한글화 및 폰트 확대)
    fig.add_annotation(x=4.5, y=plot_df['Monetary'].max()*0.9, text="<b>핵심 우량 고객</b>", showarrow=False, font=dict(size=18, color="#059669"))
    fig.add_annotation(x=1.5, y=plot_df['Monetary'].max()*0.9, text="<b>중점 관리(이탈 위험)</b>", showarrow=False, font=dict(size=18, color="#dc2626"))
    fig.add_annotation(x=4.5, y=m_standard*0.4, text="<b>성장 잠재 고객</b>", showarrow=False, font=dict(size=18, color="#2563eb"))
    fig.add_annotation(x=1.5, y=m_standard*0.4, text="<b>일반 유입 고객</b>", showarrow=False, font=dict(size=18, color="#64748b"))

    st.plotly_chart(fig, use_container_width=True)

with col_desc:
    st.subheader("🔍 세그먼트 요약 리포트")
    
    seg_stats = df_cust.groupby('Segment').agg({'Avg_Delay': 'mean', 'customer_unique_id': 'count'}).reset_index()
    
    for _, row in seg_stats.iterrows():
        color = "#059669" if row['Segment'] == '핵심 우량 고객' else "#dc2626" if row['Segment'] == '중점 관리(이탈 위험)' else "#2563eb" if row['Segment'] == '성장 잠재 고객' else "#64748b"
        with st.container():
            st.markdown(f"""
                <div class='insight-card' style='border-left-color: {color}; padding: 25px;'>
                    <h3 style='margin:0; font-size: 1.4em;'>{row['Segment']}</h3>
                    <p style='color: #475569; font-size: 1.1em; margin-top: 5px;'><b>규모:</b> {row['customer_unique_id']:,}명</p>
                    <p style='font-size: 1.1em;'><b>평균 배송 지연:</b> <span style='color: {color}; font-weight: bold;'>{row['Avg_Delay']:.1f}일</span></p>
                </div>
            """, unsafe_allow_html=True)
    
    st.info("💡 **그래프 읽는 법**: 점의 크기가 클수록 배송 지연이 심한 고객입니다. 좌측 상단에 큰 점이 많을수록 판매자 물류 관리가 시급함을 의미합니다.")

# 6. 페르소나 정의 및 전략 가이드 (한글 고도화)
st.divider()
st.subheader("🎭 Olist 구매자 페르소나 리포트")

p1, p2 = st.columns(2)

with p1:
    st.markdown("""
    ### 🥇 [핵심 우량 고객] 수익 창출의 핵심
    - **특징:** 높은 구매력과 만족도를 모두 갖춘 로열티 높은 그룹입니다.
    - **배송 품질:** 주로 정시 배송 비율이 높은 우수 판매자 제품을 구매합니다.
    - **핵심 전략:** VIP 전용 빠른 배송 프로모션과 '배송 안심 알림'을 통해 현 수준의 기대를 계속 충족시켜야 합니다.
    
    ### 🧨 [중점 관리 고객] 고위험 이탈군
    - **특징:** 구매 금액은 크지만 만족도가 낮아 이탈 확률이 매우 높은 그룹입니다.
    - **배송 품질:** **'불안정 성장 판매자'**로부터 잦은 배송 지연을 경험했을 가능성이 큽니다.
    - **핵심 전략:** 즉각적인 사후 보상(바우처 제공)과 지연 원인 조사가 필요하며, 고가 제품 판매 시 물류 프로세스를 재점검해야 합니다.
    """)

with p2:
    st.markdown("""
    ### 🚀 [성장 잠재 고객] 가성비를 찾는 팬덤
    - **특징:** 아직 구매액은 적지만 서비스에 만족하며 긍정적인 경험을 쌓고 있습니다.
    - **배송 품질:** 단가 대비 만족스러운 속도의 배송을 경험하고 있는 상태입니다.
    - **핵심 전략:** 재구매 주기를 단축할 수 있는 큐레이션 메일과 타임 세일을 통해 고단가 제품으로의 전환을 유도하세요.
    
    ### ⚠️ [일반 유입 고객] 탐색 단계의 신규 고객
    - **특징:** 낮은 구매액과 만족도를 보이는 그룹으로 첫인상이 좋지 않은 편입니다.
    - **배송 품질:** 초기 판매자의 운영 미숙이나 원거리 배송 지연의 영향을 주로 받습니다.
    - **핵심 전략:** 부정 리뷰 작성 가능성이 높으므로 사은품 증정이나 배송비 페이백 등 감성적인 해결책으로 재방문 의사를 높여야 합니다.
    """)

# 7. 판매자 대조 인사이트 (보충 내용)
st.success("""
### 🎯 판매자-구매자 시너지 인사이트
**'불안정 성장 판매자'**의 매출 비중이 높아질수록, 매트릭스의 **Critical Risk(좌상단)** 영역이 급격히 팽창합니다. 
이는 플랫폼 전체의 LTV(고객 생애 가치)를 갉아먹는 행위입니다. 매출 증대 전략 시 반드시 해당 판매자의 
**'배송 지연 일수 및 고객 리뷰 연동'**을 모니터링하여 Critical Risk 고객을 Premium Core로 이동시키는 물류 효율화 작업이 선행되어야 합니다.
""")

st.caption("Olist Data Analysis Dashboard v2.0 | 통합 경험-가치 매트릭스 리포트")
