import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import json

# 1. 페이지 설정 및 프리미엄 스타일링
st.set_page_config(page_title="Olist 구매자 통합 분석 및 물류 위험 지도", layout="wide")

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
    .risk-alert {
        padding: 15px; background-color: #fff5f5; border-left: 5px solid #e53e3e;
        border-radius: 8px; margin-top: 10px;
    }
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
    df = orders.merge(customers[['customer_id', 'customer_unique_id', 'customer_state']], on='customer_id')
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
    
    # RFM 등급 부여
    m_thresholds = cust_master['Monetary'].quantile([0.7, 0.9]).values
    def rfm_grade(m):
        if m >= m_thresholds[1]: return 'VIP 고객'
        elif m >= m_thresholds[0]: return '충성 고객'
        else: return '일반 고객'
    cust_master['RFM_Grade'] = cust_master['Monetary'].apply(rfm_grade)
    
    # 주(State)별 집계
    state_agg = df.groupby('customer_state').agg({
        'delay_days': 'mean',
        'review_score': 'mean',
        'price': 'sum'
    }).rename(columns={
        'delay_days': 'Avg_Delay',
        'review_score': 'Avg_Review',
        'price': 'Total_Sales'
    }).reset_index()
    
    return cust_master, state_agg

try:
    df_cust, df_state = load_data()
except Exception as e:
    st.error(f"데이터 정합성 오류: {e}")
    st.stop()

# 3. 사이드바 컨트롤
st.sidebar.header("🎯 전략적 필터링")
m_standard = st.sidebar.slider("매출 임계값 (Monetary)", 0, int(df_cust['Monetary'].quantile(0.95)), int(df_cust['Monetary'].median()))
s_standard = st.sidebar.slider("만족도 임계값 (Review Score)", 1.0, 5.0, 3.8, 0.1)

# 세그먼트 분류 로직
def classify(row):
    if row['Monetary'] >= m_standard:
        return '핵심 우량 고객' if row['Satisfaction'] >= s_standard else '중점 관리(이탈 위험)'
    else:
        return '성장 잠재 고객' if row['Satisfaction'] >= s_standard else '일반 유입 고객'

df_cust['Segment'] = df_cust.apply(classify, axis=1)

# 4. 헤더 섹션
st.title("🛡️ Olist 구매자 통합 분석 및 물류 위험 지도")
st.markdown("구매자 가치-경험 매트릭스와 지역별 물류 위험도를 결합하여 입체적인 전략을 제시합니다.")

# 지표 요약
m1, m2, m3, m4 = st.columns(4)
m1.metric("총 분석 구매자", f"{len(df_cust):,}")
m2.metric("평균 만족도", f"{df_cust['Satisfaction'].mean():.2f} ⭐")
m3.metric("평균 지연 일수", f"{df_cust['Avg_Delay'].mean():.1f} 일")
m4.metric("VIP 비중", f"{(df_cust['RFM_Grade']=='VIP 고객').mean()*100:.1f}%")

st.divider()

# 5. 구매자 가치-경험 매트릭스
col_vis, col_desc = st.columns([2, 1])
with col_vis:
    st.subheader("📍 고객 경험-가치 매트릭스 시각화")
    plot_df = df_cust.sample(min(len(df_cust), 5000), random_state=42).copy()
    plot_df['Avg_Delay_Plot'] = plot_df['Avg_Delay'].fillna(0).clip(lower=0.1)
    
    fig = px.scatter(
        plot_df, x='Satisfaction', y='Monetary', color='RFM_Grade', size='Avg_Delay_Plot',
        hover_name='customer_unique_id',
        hover_data={'Segment': True, 'Primary_Category': True, 'Frequency': True, 'Avg_Delay': ':.1f', 'Avg_Delay_Plot': False},
        color_discrete_map={'VIP 고객': '#1A3A5F', '충성 고객': '#3A7CA5', '일반 고객': '#A2C4D8'},
        labels={'Satisfaction': '배송 만족도', 'Monetary': '총 구매 가치', 'RFM_Grade': '고객 등급'},
        height=650, template="plotly_white", size_max=35
    )
    fig.update_layout(font=dict(size=14))
    fig.add_vline(x=s_standard, line_dash="dash", line_color="#cbd5e1")
    fig.add_hline(y=m_standard, line_dash="dash", line_color="#cbd5e1")
    fig.add_annotation(x=4.5, y=plot_df['Monetary'].max()*0.9, text="<b>핵심 우량 고객</b>", showarrow=False, font=dict(size=18, color="#059669"))
    fig.add_annotation(x=1.5, y=plot_df['Monetary'].max()*0.9, text="<b>중점 관리(이탈)</b>", showarrow=False, font=dict(size=18, color="#dc2626"))
    fig.add_annotation(x=4.5, y=m_standard*0.4, text="<b>성장 잠재 고객</b>", showarrow=False, font=dict(size=18, color="#2563eb"))
    fig.add_annotation(x=1.5, y=m_standard*0.4, text="<b>일반 유입 고객</b>", showarrow=False, font=dict(size=18, color="#64748b"))
    st.plotly_chart(fig, use_container_width=True)

with col_desc:
    st.subheader("🔍 세그먼트 요약 리포트")
    seg_stats = df_cust.groupby('Segment').agg({'Avg_Delay': 'mean', 'customer_unique_id': 'count'}).reset_index()
    for _, row in seg_stats.iterrows():
        color = "#059669" if row['Segment'] == '핵심 우량 고객' else "#dc2626" if row['Segment'] == '중점 관리(이탈 위험)' else "#2563eb" if row['Segment'] == '성장 잠재 고객' else "#64748b"
        st.markdown(f"<div class='insight-card' style='border-left-color: {color}; padding: 15px;'><h4>{row['Segment']}</h4><p>규모: {row['customer_unique_id']:,}명</p><p>평균 지연: {row['Avg_Delay']:.1f}일</p></div>", unsafe_allow_html=True)

st.divider()

# 6. 주(State)별 물류 위험 지도
st.subheader("🗺️ 브라질 주(State)별 물류 위험 지도")
st.markdown("색상은 **평균 평점**(빨간색일수록 위험), 버블 크기는 **총 매출액**을 나타냅니다.")

# 브라질 주 센터 좌표 (시각화용 대략적 위치)
state_coords = {
    'AC': [-9.02, -70.81], 'AL': [-9.57, -36.78], 'AP': [1.41, -51.77], 'AM': [-3.47, -62.21],
    'BA': [-12.97, -38.51], 'CE': [-3.71, -38.54], 'DF': [-15.78, -47.93], 'ES': [-19.19, -40.34],
    'GO': [-16.68, -49.25], 'MA': [-2.53, -44.30], 'MT': [-12.64, -55.42], 'MS': [-20.44, -54.64],
    'MG': [-18.51, -44.55], 'PA': [-1.45, -48.50], 'PB': [-7.11, -34.86], 'PR': [-25.42, -49.27],
    'PE': [-8.05, -34.88], 'PI': [-5.09, -42.80], 'RJ': [-22.90, -43.17], 'RN': [-5.79, -35.21],
    'RS': [-30.03, -51.23], 'RO': [-8.76, -63.90], 'RR': [2.82, -60.67], 'SC': [-27.59, -48.54],
    'SP': [-23.55, -46.63], 'SE': [-10.91, -37.07], 'TO': [-10.17, -48.33]
}

df_state['lat'] = df_state['customer_state'].map(lambda x: state_coords.get(x, [0,0])[0])
df_state['lon'] = df_state['customer_state'].map(lambda x: state_coords.get(x, [0,0])[1])

# 지도 시각화 (Scattergeo with Choropleth-like feel)
fig_map = px.scatter_geo(
    df_state, lat='lat', lon='lon', color='Avg_Review', size='Total_Sales',
    hover_name='customer_state', size_max=40,
    color_continuous_scale='Reds_r', # 평점이 낮을수록 진한 빨간색
    range_color=[3.0, 4.5],
    labels={'Avg_Review': '평균 평점', 'Total_Sales': '총 매출액', 'Avg_Delay': '평균 지연'},
    hover_data={'Avg_Delay': ':.1f', 'lat': False, 'lon': False},
    projection="natural earth",
    title="주별 매출 규모 vs 서비스 만족도"
)
fig_map.update_geos(scope='south america', showcountries=True, countrycolor="lightgray", showlakes=False)
fig_map.update_layout(height=600, margin={"r":0,"t":50,"l":0,"b":0})

st.plotly_chart(fig_map, use_container_width=True)

# 7. 물류 요주의 지역 분석 및 경고
st.divider()
st.subheader("⚠️ 물류 요주의 지역 및 대조 분석")

high_risk_states = df_state[df_state['Avg_Review'] < 3.8].sort_values('Total_Sales', ascending=False)
top_risk = high_risk_states.iloc[0]['customer_state'] if not high_risk_states.empty else "None"

c_risk1, c_risk2 = st.columns(2)
with c_risk1:
    st.markdown(f"""
    #### 🚩 물류 요주의 지역 (Logistics Critical Zones)
    매출 규모는 크지만 서비스 만족도가 낮은 지역입니다.
    - **최고 위험 지역:** `{top_risk}` (매출 대비 낮은 서비스 지수)
    - **관리 필요 지역:** {', '.join(high_risk_states['customer_state'].tolist()[:3])}
    
    이 지역들은 플랫폼의 핵심 수익원이지만 **'불안정 성장 판매자'**의 영향을 가장 많이 받아 고객 이탈이 가속화되고 있습니다.
    """)

with c_risk2:
    # 특정 위험 지역 경고 오토 제너레이션
    warn_text = ""
    if 'AL' in df_state['customer_state'].values or 'MA' in df_state['customer_state'].values:
        remote_states = df_state[df_state['customer_state'].isin(['AL', 'MA'])]
        for _, r in remote_states.iterrows():
            if r['Avg_Delay'] > 15:
                warn_text += f"**{r['customer_state']} 지역 경고:** 평균 배송 지연 {r['Avg_Delay']:.1f}일로 임계치 초과. "
    
    st.markdown(f"""
    <div class='risk-alert'>
        <strong>🚨 시스템 자동 경고:</strong><br>
        {warn_text if warn_text else "현재 특이 지연 지역이 식별되지 않았습니다."}<br><br>
        AL, MA 등의 지역은 배송 편차가 매우 커서 <strong>'저가치 불만족군'</strong>을 대량 양산하고 있습니다. 
        신규 판매자의 무리한 지역 확장보다는 안정적인 지역(SP, RJ) 위주의 배송 허브 구축이 시급합니다.
    </div>
    """, unsafe_allow_html=True)

# 8. Olist 구매자 통합 페르소나 리포트 (심층 분석)
st.divider()
st.subheader("🎭 Olist 구매자 통합 페르소나 리포트")
st.markdown("데이터 분석 결과를 바탕으로 도출된 4가지 핵심 페르소나의 행동 패턴과 관리 전략입니다.")

p1, p2 = st.columns(2)

with p1:
    # 1. 핵심 우량 고객
    st.markdown("""
    <div class='insight-card' style='border-left-color: #059669; padding: 20px;'>
        <h3 style='margin:0;'>🥇 [핵심 우량 고객] 수익 창출의 절대적 지주</h3>
        <p style='margin-top:10px;'><strong>📌 핵심 지표:</strong> 높은 구매 금액(Monetary) + 높은 만족도(Satisfaction) + 매우 낮은 지연율</p>
        <p><strong>비즈니스 가치:</strong> 매출 기여도가 가장 높으며, 플랫폼의 평판을 유지하는 핵심 자산입니다.</p>
        <p><strong>행동 분석:</strong> 주로 '핵심 판매자(Core Sellers)'의 고가 가전/가구 카테고리를 이용하며, 안정적인 물류 서비스를 최우선 가치로 여깁니다.</p>
        <p><strong>심층 전략:</strong> 기대치가 매우 높은 그룹이므로 1일 이상의 지연도 치명적일 수 있습니다. 'VIP 전용 물류 루틴'을 적용하고 차별화된 리워드를 집중 배치해야 합니다.</p>
    </div>
    """, unsafe_allow_html=True)
    
    # 2. 중점 관리 고객
    st.markdown("""
    <div class='insight-card' style='border-left-color: #dc2626; padding: 20px;'>
        <h3 style='margin:0;'>🧨 [중점 관리 고객] 고위험 자산 이탈군</h3>
        <p style='margin-top:10px;'><strong>📌 핵심 지표:</strong> 높은 구매 금액(Monetary) + 높은 배송 지연(Delay Days) + 낮은 평점</p>
        <p><strong>비즈니스 가치:</strong> 고액 결제자임에도 불구하고 물류 실패로 인해 브랜드를 등질 위험이 가장 큰 그룹입니다.</p>
        <p><strong>행동 분석:</strong> 매출은 높으나 운영 관리가 부실한 <strong>'불안정 성장 판매자'</strong>의 상품을 구매했을 가능성이 매우 높습니다.</p>
        <p><strong>심층 전략:</strong> 이들은 배송 약속이 깨졌을 때 즉각 이탈합니다. 선제적 지연 예측 시스템과 사과 바우처 발급을 통해 이탈을 필사적으로 차단해야 합니다.</p>
    </div>
    """, unsafe_allow_html=True)

with p2:
    # 3. 성장 잠재 고객
    st.markdown("""
    <div class='insight-card' style='border-left-color: #2563eb; padding: 20px;'>
        <h3 style='margin:0;'>🚀 [성장 잠재 고객] 가성비를 찾는 효율적 팬덤</h3>
        <p style='margin-top:10px;'><strong>📌 핵심 지표:</strong> 낮은 구매 금액(Monetary) + 매우 높은 만족도(Satisfaction) + 우수한 물류 품질</p>
        <p><strong>비즈니스 가치:</strong> 현재 매출은 낮지만 긍정적인 경험(UX)을 축적 중인 '미래의 VIP' 후보군입니다.</p>
        <p><strong>행동 분석:</strong> 생필품, 뷰티 등 회전율이 빠르고 배송비 부담이 적은 카테고리를 선호하며, 무료 배송 혜택에 민감합니다.</p>
        <p><strong>심층 전략:</strong> 업셀링(Up-selling)이 핵심입니다. 무료 배송 임계값 설정을 통해 객단가를 높이고, VIP 세그먼트로 이동시켜야 합니다.</p>
    </div>
    """, unsafe_allow_html=True)

    # 4. 일반 유입 고객
    st.markdown("""
    <div class='insight-card' style='border-left-color: #64748b; padding: 20px;'>
        <h3 style='margin:0;'>⚠️ [일반 유입 고객] 탐색 단계의 불확실 고객</h3>
        <p style='margin-top:10px;'><strong>📌 핵심 지표:</strong> 낮은 구매 금액(Monetary) + 낮은 평점 + 긴 배송 시일</p>
        <p><strong>비즈니스 가치:</strong> 첫 구매 경험이 부정적으로 형성된 그룹으로, 플랫폼에 대한 불신이 높습니다.</p>
        <p><strong>행동 분석:</strong> 초기 진입 판매자나 원거리 물류 취약 지역 고객들이 다수 포함됩니다. 지연 발생 시 부정적 인식을 굳히는 단계입니다.</p>
        <p><strong>심층 전략:</strong> 부정적 입소문 방지가 최우선입니다. 감성적인 품질 관리(사은품 등)와 신뢰 회복 쿠폰을 통해 다시 방문할 구체적 명분을 제공해야 합니다.</p>
    </div>
    """, unsafe_allow_html=True)

st.caption("Olist Data Analysis Dashboard v2.5 | 통합 경험-가치 및 리스크 맵 리포트")
