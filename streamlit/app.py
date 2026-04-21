"""
Saudi RE Market Intelligence — Bilingual Dashboard (AR/EN with RTL support)
"""
from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(
    page_title="Saudi RE Market Intelligence | تحليل السوق العقاري السعودي",
    page_icon="🏠", layout="wide", initial_sidebar_state="expanded",
)

# ─── Global UI styling ───────────────────────────────────────────
st.markdown("""
<style>
[data-testid="stMetric"] {
    background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
    border: 1px solid #2d6a4f33;
    border-radius: 12px;
    padding: 16px 20px;
    box-shadow: 0 2px 8px rgba(0,0,0,0.3);
}
[data-testid="stMetricValue"] { font-size: 1.6rem !important; font-weight: 700 !important; }
[data-testid="stMetricLabel"] { font-size: 0.85rem !important; opacity: 0.8; }
div.stAlert > div { border-radius: 10px !important; }
.legend-hint { color: #888; font-size: 0.8em; font-style: italic; text-align: center; margin-top: -8px; }
</style>
""", unsafe_allow_html=True)

DATA_DIR = Path(__file__).parent

@st.cache_data
def load_data():
    # Cache invalidation comment: v2
    return (
        pd.read_csv(DATA_DIR / "fct_seasonal_patterns.csv"),
        pd.read_csv(DATA_DIR / "fct_emerging_cities.csv"),
        pd.read_csv(DATA_DIR / "fct_deal_size_trends.csv"),
    )

seasonal_df, emerging_df, deal_size_df = load_data()

# ─── City transliteration (top 30 + major cities) ────────────────
CITY_EN = {
    "الروضة": "Al Rawdah", "الكهفه": "Al Kahfah", "بيش": "Baysh", "موقق": "Mawqaq",
    "الظهران": "Dhahran", "الغزاله": "Al Ghazalah", "سميراء": "Samira",
    "وادي الفرع": "Wadi Al Fara", "بقعاء": "Buqaa", "السليمي": "Al Sulaymi",
    "الجبيل": "Jubail", "المذنب": "Al Mithnab", "قرية العليا": "Qaryat Al Ulya",
    "الشنان": "Al Shannan", "الغاط": "Al Ghat", "ضمد": "Damad", "الجموم": "Al Jumum",
    "خليص": "Khulays", "جلاجل": "Jalajil", "جده": "Jeddah", "الدمام": "Dammam",
    "المدينة المنورة": "Madinah", "الدرب": "Al Darb", "مرات": "Marat",
    "مكة المكرمة": "Makkah", "حريملاء": "Huraymila", "الشماسيه": "Al Shimasiyah",
    "الرين": "Al Rayn", "باللسمر": "Ballasmar", "الطائف": "Taif",
    "الرياض": "Riyadh", "بريده": "Buraydah", "تبوك": "Tabuk", "ابها": "Abha",
    "حائل": "Hail", "نجران": "Najran", "عنيزه": "Unayzah", "الخبر": "Khobar",
    "الاحساء": "Al Ahsa", "ينبع": "Yanbu", "الباحة": "Al Baha", "القطيف": "Qatif",
    "سكاكا": "Sakaka", "عرعر": "Arar", "الخرج": "Al Kharj", "الزلفي": "Zulfi",
    "شقراء": "Shaqra", "المجمعه": "Al Majmaah", "القويعيه": "Al Quwaiyah",
    "عفيف": "Afif", "الدوادمي": "Al Dawadmi", "الدرعيه": "Diriyah",
    "المزاحميه": "Al Muzahimiyah", "رماح": "Rumah", "ضرماء": "Dhurma",
    "الافلاج": "Al Aflaj", "صبياء": "Sabya", "ابو عريش": "Abu Arish",
    "صامطه": "Samtah", "رابغ": "Rabigh", "الليث": "Al Lith",
    "القنفذة": "Al Qunfudhah", "المجارده": "Al Majardah", "بلجرشي": "Baljurashi",
    "رفحاء": "Rafha", "طريف": "Turaif", "حفر الباطن": "Hafar Al Batin",
    "القريات": "Al Qurayyat", "دومة الجندل": "Dumat Al Jandal",
    "الوجه": "Al Wajh", "تيماء": "Tayma", "حقل": "Haql", "ضبا": "Duba",
    "بدر": "Badr", "العلا": "Al Ula", "محايل": "Mahayil", "خميس مشيط": "Khamis Mushait",
    "النماص": "Al Namas", "بيشه": "Bisha", "المخواة": "Al Makhwah",
    "الحريق": "Al Hariq", "ثادق": "Thadiq", "الدلم": "Al Dilam",
    "حوطة بني تميم": "Hawtat Bani Tamim", "القصب": "Al Qasab",
    "الارطاوي": "Al Artawi", "الارطاويه": "Al Artawiyah", "عرقه": "Irqah",
    "السليل": "Al Sulayyil", "وادي الدواسر": "Wadi Al Dawasir",
    "احد المسارحه": "Ahad Al Masarihah", "العيدابي": "Al Aidabi",
    "فيفاء": "Fayfa", "هروب": "Harub", "الحرث": "Al Harith",
    "الريث": "Al Rayth", "العارضه": "Al Aridah", "الرس": "Al Rass",
    "البكيريه": "Al Bukayriyah", "الخبراء": "Al Khabra",
    "البدائع": "Al Badai", "رياض الخبراء": "Riyadh Al Khabra",
    "المجمعة": "Al Majmaah", "عيون الجواء": "Uyun Al Jawa",
    "النبهانيه": "Al Nabhaniyah", "الشقه": "Al Shiqqah",
    "ابانات": "Abanat", "الحائط": "Al Hait", "الموسم": "Al Mawsim",
}

# ─── Translations ─────────────────────────────────────────────────
T = {
  "en": {
    "title": "🏠 Saudi Real Estate Market Intelligence",
    "subtitle": "Analysing 1.4M MOJ sale transactions across 13 regions and 172 cities (2020–2025)",
    "problem_title": "📌 What Are We Solving?",
    "problem_text": (
        "Saudi Arabia's real estate market generated **over 1.4 trillion SAR** in transactions "
        "between 2020 and 2025, yet investors and developers lack a clear, data-driven lens "
        "for three critical decisions:\n"
        "1. **When** to transact — which quarters consistently see higher activity?\n"
        "2. **Where** to invest — which secondary cities are rapidly growing?\n"
        "3. **What** is happening to deal sizes — are prices and lot sizes rising or shrinking?"
    ),
    "s_title": "📊 Tile 1 · Seasonal Patterns — Which Quarters Outperform?",
    "s_insight": "**How to read this:** Each cell shows how many deals happened on average in that quarter for that region. **Darker green = more deals.** Look for patterns — if Q4 is always dark, that's a consistently busy season. Buy during lighter quarters when competition is lower.",
    "s_desc": "Average quarterly transaction count by region (2020–2025).",
    "e_title": "🚀 Tile 2 · Emerging Cities — Fastest-Growing Markets",
    "e_insight": "**How to read this:** Cities are ranked by their yearly growth rate (CAGR = Compound Annual Growth Rate — how fast the number of deals grows each year, compounded). A city with 50% CAGR roughly doubles its deals every 2 years. These are small cities growing fast — early investment opportunities.",
    "e_desc": "Cities ranked by yearly growth rate. Only cities with 10+ deals in their first year are shown.",
    "p_title": "💰 Tile 3 · Median Deal Price Over Time",
    "p_insight": "**How to read this:** Each point is the middle price of all deals that quarter (the median — half of deals are above, half below). Rising lines = prices going up. Notice the 2023 dip and 2024 recovery. \n Click any legend item to show/hide that property type.",
    "p_desc": "Quarterly median transaction price (SAR) by property type, for selected regions.",
    "a_title": "📐 Tile 4 · Median Property Area Over Time",
    "a_insight": "**How to read this:** Each point is the middle lot size (m²) that quarter. If areas shrink while prices rise, each square metre is getting more expensive — a key affordability signal. \n Click any legend item to show/hide that property type.",
    "a_desc": "Quarterly median property area (m²) by property type, for selected regions.",
    "legend_hint": "💡 Tip: Click on a legend item to show/hide that category.",
    "d_title": "🎯 How to Use These 4 Tiles Together For a Decision",
    "d_text": (
        "1. **Tile 1** → **When** — best quarter to buy in your target region.\n"
        "2. **Tile 2** → **Where** — under-the-radar cities before they peak.\n"
        "3. **Tiles 3 & 4** → **What** — rising or cooling market? Lot sizes growing or shrinking?\n\n"
        "**Example:** Q1 is slow in Al Baha (Tile 1), Al Rawdah in Hail grows 52%/yr (Tile 2), "
        "residential prices recovering post-2023 (Tile 3). → Buy in emerging Hail cities during Q1."
    ),
    "quarter": "Quarter", "region": "Region", "txns": "Avg Transactions / Quarter",
    "city": "City", "cagr": "CAGR (%)", "med_price": "Median Price (SAR)",
    "med_area": "Median Area (m²)", "cls": "Classification", "yq": "Year-Quarter",
    "kpi1": "Total Transactions", "kpi2": "Total Value (B SAR)",
    "kpi3": "Regions", "kpi4": "Cities Analysed",
    "top_n": "🚀 Tile 2: Show top N cities", "filter_region": "Filter by Region (all tiles)", "filter_year": "Filter by Year (Tile 1)",
    "src": "Data: MOJ Saudi Arabia (2020–2025) | ", "repo": "GitHub Repository",
  },
  "ar": {
    "title": "🏠 تحليل السوق العقاري السعودي",
    "subtitle": "تحليل أكثر من ١.٤ مليون صفقة بيع من وزارة العدل عبر ١٣ منطقة و١٧٢ مدينة (٢٠٢٠–٢٠٢٥)",
    "problem_title": "📌 ما المشكلة التي نعالجها؟",
    "problem_text": (
        "حقق سوق العقارات السعودي أكثر من **١.٤ تريليون ريال** بين ٢٠٢٠ و٢٠٢٥، "
        "لكن المستثمرين يفتقرون لرؤية مبنية على البيانات لثلاثة قرارات:\n"
        "1. **متى** يشترون — أي الأرباع الأكثر نشاطاً؟\n"
        "2. **أين** يستثمرون — أي المدن الثانوية تنمو بسرعة؟\n"
        "3. **ماذا** يحدث لأحجام الصفقات — هل الأسعار والمساحات ترتفع أم تنخفض؟"
    ),
    "s_title": "📊 لوحة ١ · التوزيع الموسمي — أي الأرباع الأكثر نشاطاً؟",
    "s_insight": "**كيف تقرأ هذا:** كل خلية تُظهر متوسط عدد الصفقات في ذلك الربع لتلك المنطقة. **الأخضر الداكن = صفقات أكثر.** ابحث عن الأنماط — إذا كان الربع الرابع دائماً داكناً فهو موسم الذروة. اشترِ في الأرباع الأفتح حين تقل المنافسة.",
    "s_desc": "متوسط عدد الصفقات لكل ربع حسب المنطقة (٢٠٢٠–٢٠٢٥)",
    "e_title": "🚀 لوحة ٢ · المدن الناشئة — أسرع الأسواق نمواً",
    "e_insight": "**كيف تقرأ هذا:** المدن مرتبة حسب سرعة نموها السنوي (معدل النمو المركب CAGR — كم تنمو الصفقات سنوياً بشكل مركّب). مدينة بنسبة ٥٠٪ تضاعف صفقاتها كل سنتين تقريباً. هذه مدن صغيرة تنمو بسرعة — فرص استثمارية مبكرة.",
    "e_desc": "المدن مرتبة حسب معدل النمو السنوي. فقط المدن التي لديها ١٠+ صفقات في السنة الأولى.",
    "p_title": "💰 لوحة ٣ · اتجاه أسعار الصفقات",
    "p_insight": "**كيف تقرأ هذا:** كل نقطة هي السعر المتوسط (الوسيط — نصف الصفقات أعلى ونصف أقل). الخط الصاعد = الأسعار ترتفع. لاحظ انخفاض ٢٠٢٣ وتعافي ٢٠٢٤. انقر على عنصر في المفتاح لإظهاره أو إخفائه.",
    "p_desc": "الوسيط الفصلي لسعر الصفقة (ريال) حسب نوع العقار، للمناطق المختارة",
    "a_title": "📐 لوحة ٤ · اتجاه مساحات العقارات",
    "a_insight": "**كيف تقرأ هذا:** كل نقطة هي المساحة المتوسطة (م²) في ذلك الربع. إذا تقلصت المساحات وارتفعت الأسعار فسعر المتر المربع يتسارع — مؤشر على تراجع القدرة الشرائية. انقر على عنصر في المفتاح لإظهاره أو إخفائه.",
    "a_desc": "الوسيط الفصلي لمساحة العقار (م²) حسب نوع العقار، للمناطق المختارة",
    "legend_hint": "💡 نصيحة: انقر على عنصر في المفتاح لإظهائه أو إخفائه.",
    "d_title": "🎯 كيف تستخدم اللوحات الأربع معاً؟",
    "d_text": (
        "1. **لوحة ١** ← **متى** — أفضل ربع للشراء في منطقتك.\n"
        "2. **لوحة ٢** ← **أين** — مدن واعدة قبل ارتفاع أسعارها.\n"
        "3. **لوحتا ٣ و ٤** ← **ماذا** — سوق صاعد أم متراجع؟ مساحات تكبر أم تصغر؟\n\n"
        "**مثال:** الربع الأول هادئ في الباحة (لوحة ١)، الروضة في حائل تنمو ٥٢٪ سنوياً (لوحة ٢)، "
        "أسعار السكني تتعافى (لوحة ٣). ← اشترِ أراضٍ سكنية في مدن حائل الناشئة خلال الربع الأول."
    ),
    "quarter": "الربع", "region": "المنطقة", "txns": "متوسط الصفقات / الربع",
    "city": "المدينة", "cagr": "معدل النمو (%)", "med_price": "الوسيط (ريال)",
    "med_area": "الوسيط (م²)", "cls": "التصنيف", "yq": "السنة-الربع",
    "kpi1": "إجمالي الصفقات", "kpi2": "القيمة (مليار ر.س)",
    "kpi3": "المناطق", "kpi4": "المدن",
    "top_n": "🚀 لوحة ٢: عدد المدن المعروضة", "filter_region": "تصفية حسب المنطقة (كل اللوحات)", "filter_year": "تصفية حسب السنة (لوحة ١)",
    "src": "البيانات: وزارة العدل (٢٠٢٠–٢٠٢٥) | ", "repo": "GitHub",
  },
}

CLASS_EN = {"سكني": "Residential", "تجاري": "Commercial", "زراعي": "Agricultural", "صناعي": "Industrial"}
CLASS_AR = {v: k for k, v in CLASS_EN.items()}
CLASS_COLORS = {"Residential": "#2D6A4F", "Commercial": "#E76F51", "Agricultural": "#E9C46A", "Industrial": "#264653"}
CLASS_COLORS_AR = {CLASS_AR[k]: v for k, v in CLASS_COLORS.items()}
REGION_COLORS = px.colors.qualitative.Set3

# Region lookup
_rp = seasonal_df[["region_en", "region_ar"]].drop_duplicates()
EN2AR = dict(zip(_rp["region_en"], _rp["region_ar"]))
AR2EN = dict(zip(_rp["region_ar"], _rp["region_en"]))

# ─── Sidebar ──────────────────────────────────────────────────────
with st.sidebar:
    lang = st.radio("🌐 Language | اللغة", ["English", "العربية"], horizontal=True)
    is_ar = lang == "العربية"
    t = T["ar" if is_ar else "en"]
    st.divider()

    # Region filter — always English internally
    all_en = sorted(seasonal_df["region_en"].unique())
    display = [EN2AR.get(r, r) for r in all_en] if is_ar else all_en

    sel = st.multiselect(t["filter_region"], options=display, default=display, key="region_multi")
    sel_en = [AR2EN.get(r, r) for r in sel] if is_ar else sel

    st.divider()
    st.markdown(f"*{t['src']}[{t['repo']}](https://github.com/civillizard/Saudi-Real-Estate-Data)*")

# ─── RTL CSS injection for Arabic ─────────────────────────────────
if is_ar:
    st.markdown("""<style>
    .main .block-container, .stSidebar, .stMarkdown, .stExpander,
    [data-testid="stSidebar"], [data-testid="stMetricValue"],
    [data-testid="stMetricLabel"], .stMultiSelect, .stSlider,
    h1, h2, h3, h4, p, li, span, label, div.stAlert {
        direction: rtl !important;
        text-align: right !important;
    }
    </style>""", unsafe_allow_html=True)

# ─── Header + KPIs ───────────────────────────────────────────────
st.markdown(f"# {t['title']}")
st.markdown(f"*{t['subtitle']}*")

total_v = deal_size_df["total_value"].sum() / 1e9
c1, c2, c3, c4 = st.columns(4)
c1.metric(t["kpi1"], f"{1_412_119:,}")
c2.metric(t["kpi2"], f"{total_v:,.1f}")
c3.metric(t["kpi3"], str(seasonal_df["region_en"].nunique()))
c4.metric(t["kpi4"], str(emerging_df["city"].nunique()))
st.divider()

with st.expander(t["problem_title"], expanded=False):
    st.markdown(t["problem_text"])

with st.expander(t["d_title"], expanded=False):
    st.markdown(t["d_text"])
st.divider()

# ═══════════════════════════════════════════════════════════════════
# TILE 1 — Seasonal Heatmap (filtered by region)
# ═══════════════════════════════════════════════════════════════════
st.subheader(t["s_title"]); st.caption(t["s_desc"]); st.info(t["s_insight"])

# Year filter for Tile 1
all_years = sorted(seasonal_df["year"].dropna().unique().astype(int).tolist())
sel_years = st.multiselect(t["filter_year"], options=all_years, default=all_years, key="year_multi")

filt_s = seasonal_df[
    (seasonal_df["region_en"].isin(sel_en)) & 
    (seasonal_df["year"].isin(sel_years))
].copy()
rcol = "region_ar" if is_ar else "region_en"

# Group by region and quarter, take the mean across selected years
agg_s = filt_s.groupby([rcol, "quarter"], dropna=False).agg(
    avg_quarterly_transactions=("transaction_count", "mean")
).reset_index()

piv = agg_s.pivot_table(index=rcol, columns="quarter", values="avg_quarterly_transactions", aggfunc="sum").fillna(0)
piv.columns = [f"Q{int(q)}" for q in piv.columns]
fig1 = go.Figure(go.Heatmap(
    z=piv.values, x=piv.columns.tolist(), y=piv.index.tolist(),
    colorscale="YlGn", text=piv.values.astype(int).astype(str),
    texttemplate="%{text}", textfont={"size": 12},
    hovertemplate=f"{t['region']}: %{{y}}<br>{t['quarter']}: %{{x}}<br>{t['txns']}: %{{z:,.0f}}<extra></extra>",
    colorbar=dict(title=t["txns"]),
))
fig1.update_layout(height=max(350, len(sel_en)*45), margin=dict(l=0,r=0,t=30,b=0),
                   xaxis_title=t["quarter"], yaxis_title=t["region"], font=dict(size=13))
st.plotly_chart(fig1, use_container_width=True)
st.divider()

# ═══════════════════════════════════════════════════════════════════
# TILE 2 — Emerging Cities (filtered by region + top_n)
# ═══════════════════════════════════════════════════════════════════
st.subheader(t["e_title"]); st.caption(t["e_desc"]); st.info(t["e_insight"])

top_n = st.slider(t["top_n"], min_value=5, max_value=30, value=15)

filt_e = emerging_df[emerging_df["region_en"].isin(sel_en)].head(top_n).copy()
filt_e["cagr_pct"] = filt_e["transaction_cagr"] * 100

# Translate city names
if is_ar:
    filt_e["city_display"] = filt_e["city"]  # already Arabic
    rlbl = "region_ar"
else:
    filt_e["city_display"] = filt_e["city"].map(CITY_EN).fillna(filt_e["city"])
    rlbl = "region_en"

fig2 = px.bar(filt_e, x="cagr_pct", y="city_display", color=rlbl, orientation="h",
    labels={"cagr_pct": t["cagr"], "city_display": t["city"], rlbl: t["region"]},
    color_discrete_sequence=REGION_COLORS, text="cagr_pct")
fig2.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
fig2.update_layout(height=max(400, top_n*30), margin=dict(l=0,r=0,t=30,b=0),
    yaxis=dict(categoryorder="total ascending"), showlegend=True,
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1), font=dict(size=12))
st.plotly_chart(fig2, use_container_width=True)
st.divider()

# ═══════════════════════════════════════════════════════════════════
# Prepare deal_size data (filtered by region, aggregated back to national)
# ═══════════════════════════════════════════════════════════════════
filt_d = deal_size_df[deal_size_df["region_en"].isin(sel_en)].copy()
# Re-aggregate across regions for the selected filter
agg_d = filt_d.groupby(["year_quarter", "year", "quarter", "classification_ar", "classification_en"], dropna=False).agg(
    transaction_count=("transaction_count", "sum"),
    total_value=("total_value", "sum"),
    median_price=("median_price", "median"),
    median_area=("median_area", "median"),
).reset_index()
agg_d["q_label"] = agg_d["year"].astype(int).astype(str) + "-Q" + agg_d["quarter"].astype(int).astype(str)

if is_ar:
    agg_d["cls"] = agg_d["classification_ar"]
    cmap = CLASS_COLORS_AR
else:
    agg_d["cls"] = agg_d["classification_en"]
    cmap = CLASS_COLORS

# ═══════════════════════════════════════════════════════════════════
# TILE 3 — Median Price (filtered by region)
# ═══════════════════════════════════════════════════════════════════
st.subheader(t["p_title"]); st.caption(t["p_desc"]); st.info(t["p_insight"])
fig3 = px.line(agg_d, x="q_label", y="median_price", color="cls", markers=True,
    labels={"q_label": t["yq"], "median_price": t["med_price"], "cls": t["cls"]},
    color_discrete_map=cmap)
fig3.update_layout(height=400, margin=dict(l=0,r=0,t=30,b=0),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    font=dict(size=12), xaxis_tickangle=-45)
st.plotly_chart(fig3, use_container_width=True)
st.markdown(f"<p class='legend-hint'>{t.get('legend_hint','')}</p>", unsafe_allow_html=True)
st.divider()

# ═══════════════════════════════════════════════════════════════════
# TILE 4 — Median Area (filtered by region)
# ═══════════════════════════════════════════════════════════════════
st.subheader(t["a_title"]); st.caption(t["a_desc"]); st.info(t["a_insight"])
fig4 = px.line(agg_d, x="q_label", y="median_area", color="cls", markers=True,
    labels={"q_label": t["yq"], "median_area": t["med_area"], "cls": t["cls"]},
    color_discrete_map=cmap)
fig4.update_layout(height=400, margin=dict(l=0,r=0,t=30,b=0),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    font=dict(size=12), xaxis_tickangle=-45)
st.plotly_chart(fig4, use_container_width=True)
st.markdown(f"<p class='legend-hint'>{t.get('legend_hint','')}</p>", unsafe_allow_html=True)

# ─── Footer ───────────────────────────────────────────────────────
st.markdown(f"<div style='text-align:center;color:#888;font-size:.85em'>"
    f"{t['src']}<a href='https://github.com/civillizard/Saudi-Real-Estate-Data'>{t['repo']}</a></div>",
    unsafe_allow_html=True)
