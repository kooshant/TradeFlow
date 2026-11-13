
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st


# ============================================================
# 1. PAGE CONFIGURATION
# ============================================================

st.set_page_config(
    page_title="TradeFlow | Global Trade Intelligence",
    page_icon="🌐",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ============================================================
# 2. PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

ANALYTICS_DIR = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "analytics"
)


# ============================================================
# 3. PREMIUM DARK THEME
# ============================================================

st.markdown(
    """
    <style>
    .stApp {
        background-color: #0B1120;
        color: #F8FAFC;
    }

    [data-testid="stHeader"] {
        background-color: #0B1120;
    }

    [data-testid="stSidebar"] {
        background-color: #111827;
        border-right: 1px solid #263247;
    }

    [data-testid="stMetric"] {
        background-color: #111827;
        border: 1px solid #263247;
        padding: 18px;
        border-radius: 12px;
    }

    [data-testid="stMetricLabel"] {
        color: #94A3B8;
        font-size: 13px;
    }

    [data-testid="stMetricValue"] {
        color: #F8FAFC;
        font-size: 27px;
        font-weight: 700;
    }

    h1, h2, h3, h4 {
        color: #F8FAFC !important;
    }

    .hero {
        background: linear-gradient(
            120deg,
            #172554,
            #111827,
            #0F172A
        );
        border: 1px solid #263247;
        border-radius: 18px;
        padding: 30px;
        margin-bottom: 25px;
    }

    .hero-title {
        font-size: 34px;
        font-weight: 800;
        color: #F8FAFC;
        margin-bottom: 8px;
    }

    .hero-subtitle {
        font-size: 15px;
        color: #CBD5E1;
    }

    .section-label {
        color: #38BDF8;
        font-size: 12px;
        font-weight: 700;
        letter-spacing: 2px;
        margin-bottom: 8px;
    }

    .info-card {
        background-color: #111827;
        border: 1px solid #263247;
        border-radius: 12px;
        padding: 18px;
        margin-top: 10px;
        margin-bottom: 10px;
    }

    .info-card p {
        color: #CBD5E1;
        font-size: 13px;
    }

    div[data-testid="stExpander"] {
        background-color: #111827;
        border: 1px solid #263247;
        border-radius: 10px;
    }

    [data-testid="stDataFrame"] {
        border: 1px solid #263247;
        border-radius: 10px;
    }

    hr {
        border-color: #263247;
    }

    .footer {
        color: #64748B;
        text-align: center;
        font-size: 12px;
        padding: 20px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# ============================================================
# 4. PAGE HEADER
# ============================================================

st.markdown(
    """
    <div class="hero">
        <div class="hero-title">
            🌐 TradeFlow
        </div>
        <div class="hero-subtitle">
            Global Trade Intelligence Platform
            <br>
            India's International Trade Analytics | Calendar Year 2023
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)


# ============================================================
# 5. DATA LOADING
# ============================================================

@st.cache_data
def load_mart(dataset_name):

    file_path = ANALYTICS_DIR / dataset_name

    if not file_path.exists():
        raise FileNotFoundError(
            f"Analytics mart not found: {file_path}"
        )

    return pd.read_parquet(file_path)


MART_SCHEMAS = {
    "national_trade_kpis": {
        "refYear",
        "reporterDesc",
        "trade_flow",
        "trade_value_basis",
        "total_trade_value_usd",
        "record_count",
        "unique_commodity_count",
        "unique_hs_chapter_count",
    },

    "top_export_products": {
        "refYear",
        "cmdCode",
        "commodity_description",
        "product_trade_value_usd",
        "trade_value_share_pct",
        "product_rank",
    },

    "top_import_products": {
        "refYear",
        "cmdCode",
        "commodity_description",
        "product_trade_value_usd",
        "trade_value_share_pct",
        "product_rank",
    },

    "import_partner_concentration": {
        "partner2Code",
        "partner2Desc",
        "refYear",
        "partner_total_import_value_usd",
        "partner_import_share_pct",
        "partner_rank",
        "concentration_group",
    },

    "top_partner_product_mix": {
        "refYear",
        "partner2Code",
        "partner2Desc",
        "partner_rank",
        "cmdCode",
        "commodity_description",
        "partner_product_import_value_usd",
        "product_share_within_partner_pct",
        "product_rank_within_partner",
    },
}


@st.cache_data
def load_all_marts():

    datasets = {}

    for name, required_columns in MART_SCHEMAS.items():

        dataframe = load_mart(name)

        missing_columns = (
            required_columns - set(dataframe.columns)
        )

        if missing_columns:
            raise ValueError(
                f"{name} is missing columns: "
                f"{sorted(missing_columns)}"
            )

        datasets[name] = dataframe

    return datasets


try:

    marts = load_all_marts()

except Exception as error:

    st.error(f"Unable to load analytics data: {error}")
    st.stop()


# ============================================================
# 6. LOAD INDIVIDUAL DATASETS
# ============================================================

national = marts["national_trade_kpis"]

exports = marts["top_export_products"]

imports = marts["top_import_products"]

partners = marts["import_partner_concentration"]

partner_mix = marts["top_partner_product_mix"]


# ============================================================
# 7. HELPER FUNCTIONS
# ============================================================

def usd_billions(value):

    return f"${value / 1_000_000_000:,.2f}B"


def format_number(value):

    return f"{value:,.0f}"


def make_chart(
    dataframe,
    x_column,
    y_column,
    title,
    x_label,
    y_label,
    hover_columns=None,
):

    chart = px.bar(
        dataframe,
        x=x_column,
        y=y_column,
        orientation="h",
        title=title,
        labels={
            x_column: x_label,
            y_column: y_label,
        },
        hover_data=hover_columns,
        template="plotly_dark",
        color_discrete_sequence=["#38BDF8"],
    )

    chart.update_layout(
        paper_bgcolor="#111827",
        plot_bgcolor="#111827",
        font=dict(
            color="#CBD5E1",
            family="Arial",
        ),
        title_font=dict(
            size=17,
            color="#F8FAFC",
        ),
        yaxis=dict(
            categoryorder="total ascending",
            gridcolor="#263247",
            automargin=True,
        ),
        xaxis=dict(
            gridcolor="#263247",
        ),
        height=450,
        margin=dict(
            l=20,
            r=35,
            t=65,
            b=35,
        ),
        showlegend=False,
    )

    return chart


def make_product_chart(dataframe, title):

    chart_data = dataframe.sort_values(
        "product_rank",
        ascending=True,
    ).copy()

    # Keep chart labels short and readable.
    chart_data["short_description"] = (
        chart_data["commodity_description"]
        .fillna("Unknown commodity")
        .str.replace(r"\s+", " ", regex=True)
        .str.slice(0, 48)
        .str.rstrip()
    )

    chart_data["product_label"] = (
        chart_data["cmdCode"].astype(str)
        + " | "
        + chart_data["short_description"]
    )

    chart_data["trade_value_billions"] = (
        chart_data["product_trade_value_usd"]
        / 1_000_000_000
    )

    chart = px.bar(
        chart_data,
        x="trade_value_billions",
        y="product_label",
        orientation="h",
        text="trade_value_billions",
        custom_data=[
            "cmdCode",
            "commodity_description",
            "product_trade_value_usd",
            "trade_value_share_pct",
            "product_rank",
        ],
        title=title,
        labels={
            "trade_value_billions": "Trade Value (USD Billions)",
            "product_label": "Commodity (HS Code)",
        },
        template="plotly_dark",
        color_discrete_sequence=["#38BDF8"],
    )

    chart.update_traces(
        texttemplate="$%{x:.2f}B",
        textposition="outside",
        cliponaxis=False,
        hovertemplate=(
            "<b>%{customdata[1]}</b><br>"
            "HS Code: %{customdata[0]}<br>"
            "Trade Value: $%{customdata[2]:,.0f}<br>"
            "Market Share: %{customdata[3]:.2f}%<br>"
            "Product Rank: %{customdata[4]}"
            "<extra></extra>"
        ),
    )

    chart.update_layout(
        paper_bgcolor="#111827",
        plot_bgcolor="#111827",
        font=dict(
            color="#CBD5E1",
            family="Arial",
            size=11,
        ),
        title_font=dict(
            size=18,
            color="#F8FAFC",
        ),
        height=620,
        margin=dict(
            l=20,
            r=100,
            t=65,
            b=45,
        ),
        xaxis=dict(
            title="Trade Value (USD Billions)",
            gridcolor="#263247",
            rangemode="tozero",
        ),
        yaxis=dict(
            title="",
            categoryorder="total ascending",
            automargin=True,
            tickfont=dict(size=10),
        ),
        showlegend=False,
    )

    return chart


def make_partner_product_chart(dataframe, title):

    chart_data = dataframe.sort_values(
        "product_rank_within_partner",
        ascending=True,
    ).copy()

    chart_data["short_description"] = (
        chart_data["commodity_description"]
        .fillna("Unknown commodity")
        .str.replace(r"\s+", " ", regex=True)
        .str.slice(0, 48)
        .str.rstrip()
    )

    chart_data["product_label"] = (
        chart_data["cmdCode"].astype(str)
        + " | "
        + chart_data["short_description"]
    )

    chart_data["value_billions"] = (
        chart_data["partner_product_import_value_usd"]
        / 1_000_000_000
    )

    chart = px.bar(
        chart_data,
        x="value_billions",
        y="product_label",
        orientation="h",
        text="value_billions",
        custom_data=[
            "commodity_description",
            "cmdCode",
            "partner_product_import_value_usd",
            "product_share_within_partner_pct",
        ],
        title=title,
        labels={
            "value_billions": "Import Value (USD Billions)",
            "product_label": "Commodity",
        },
        template="plotly_dark",
        color_discrete_sequence=["#A78BFA"],
    )

    chart.update_traces(
        texttemplate="$%{x:.2f}B",
        textposition="outside",
        cliponaxis=False,
        hovertemplate=(
            "<b>%{customdata[0]}</b><br>"
            "HS Code: %{customdata[1]}<br>"
            "Import Value: $%{customdata[2]:,.0f}<br>"
            "Share within Partner: %{customdata[3]:.2f}%"
            "<extra></extra>"
        ),
    )

    chart.update_layout(
        paper_bgcolor="#111827",
        plot_bgcolor="#111827",
        font=dict(
            color="#CBD5E1",
            family="Arial",
            size=11,
        ),
        height=430,
        margin=dict(
            l=20,
            r=100,
            t=65,
            b=40,
        ),
        xaxis=dict(
            title="Import Value (USD Billions)",
            gridcolor="#263247",
            rangemode="tozero",
        ),
        yaxis=dict(
            title="",
            categoryorder="total ascending",
            automargin=True,
        ),
        showlegend=False,
    )

    return chart


def show_chart(chart):

    st.plotly_chart(
        chart,
        width="stretch",
        config={
            "displayModeBar": False,
            "responsive": True,
        },
    )


# ============================================================
# 8. SIDEBAR
# ============================================================

st.sidebar.markdown(
    """
    <div style="padding: 12px 0 22px 0;">
        <h2 style="color:#F8FAFC;">🌐 TradeFlow</h2>
        <p style="color:#94A3B8;">
            Global Trade Intelligence
        </p>
    </div>
    """,
    unsafe_allow_html=True,
)

st.sidebar.markdown("---")

st.sidebar.markdown("### Data Overview")

st.sidebar.info(
    """
    **Country:** India

    **Year:** 2023

    **Classification:** HS 2022

    **Processing:** Batch

    **Source:** UN Comtrade
    """
)

st.sidebar.markdown("---")

st.sidebar.caption(
    "TradeFlow Analytics Dashboard"
)


# ============================================================
# 9. NATIONAL TRADE OVERVIEW
# ============================================================

st.markdown(
    '<div class="section-label">01 / NATIONAL OVERVIEW</div>',
    unsafe_allow_html=True,
)

st.subheader("India Trade Overview")

exports_year = exports[
    exports["refYear"] == 2023
].copy()

imports_year = imports[
    imports["refYear"] == 2023
].copy()

national_year = national[
    national["refYear"] == 2023
].copy()

partners_year = partners[
    partners["refYear"] == 2023
].copy()

export_row = national_year[
    national_year["trade_flow"].str.lower() == "export"
]

import_row = national_year[
    national_year["trade_flow"].str.lower() == "import"
]

if export_row.empty or import_row.empty:
    st.error("National export/import KPI records are missing.")
    st.stop()

export_value = float(
    export_row.iloc[0]["total_trade_value_usd"]
)

import_value = float(
    import_row.iloc[0]["total_trade_value_usd"]
)

export_records = int(
    export_row.iloc[0]["record_count"]
)

import_records = int(
    import_row.iloc[0]["record_count"]
)

partner_count = int(
    partners_year["partner2Code"].nunique()
)

top_five_share = float(
    partners_year.nsmallest(
        5,
        "partner_rank",
    )["partner_import_share_pct"].sum()
)


# ============================================================
# 10. NATIONAL KPI CARDS
# ============================================================

kpi1, kpi2, kpi3 = st.columns(3)

kpi1.metric(
    "TOTAL EXPORTS · FOB",
    usd_billions(export_value),
)

kpi2.metric(
    "TOTAL IMPORTS · CIF",
    usd_billions(import_value),
)

kpi3.metric(
    "NUMBER OF IMPORT PARTNERS",
    format_number(partner_count),
)

kpi4, kpi5, kpi6 = st.columns(3)

kpi4.metric(
    "EXPORT RECORDS",
    format_number(export_records),
)

kpi5.metric(
    "IMPORT RECORDS",
    format_number(import_records),
)

kpi6.metric(
    "TOP 5 PARTNER SHARE",
    f"{top_five_share:.2f}%",
)

st.caption(
    "All values are in USD. Exports use FOB valuation and imports "
    "use CIF valuation. These valuation bases differ."
)

st.divider()


# ============================================================
# 11. EXPORT AND IMPORT PRODUCT ANALYTICS
# ============================================================

st.markdown(
    '<div class="section-label">02 / COMMODITY INTELLIGENCE</div>',
    unsafe_allow_html=True,
)

st.subheader("Product Trade Analysis")

st.markdown("#### Top Export Products")

export_chart = make_product_chart(
    exports_year,
    "Top 10 Indian Export Products — 2023",
)

show_chart(export_chart)


with st.expander("View detailed export product data"):

    st.dataframe(
        exports_year.sort_values("product_rank"),
        width="stretch",
        hide_index=True,
    )


st.markdown("#### Top Import Products")

import_chart = make_product_chart(
    imports_year,
    "Top 10 Indian Import Products — 2023",
)

show_chart(import_chart)


with st.expander("View detailed import product data"):

    st.dataframe(
        imports_year.sort_values("product_rank"),
        width="stretch",
        hide_index=True,
    )


st.divider()


# ============================================================
# 12. IMPORT PARTNER CONCENTRATION
# ============================================================

st.markdown(
    '<div class="section-label">03 / PARTNER INTELLIGENCE</div>',
    unsafe_allow_html=True,
)

st.subheader("Import Partner Analysis")

if not partners_year.empty:

    top_partners = partners_year.nsmallest(
        10,
        "partner_rank",
    )

    partner_chart = make_chart(
        top_partners,
        "partner_total_import_value_usd",
        "partner2Desc",
        "Top 10 Import Partners",
        "Import Value (USD)",
        "Partner",
        [
            "partner_import_share_pct",
            "partner_rank",
        ],
    )

    show_chart(partner_chart)

    with st.expander("View all import partners"):

        st.dataframe(
            partners_year.sort_values("partner_rank"),
            width="stretch",
            hide_index=True,
        )

else:

    st.warning("No import partner data available.")


# ============================================================
# 13. PARTNER PRODUCT INTELLIGENCE
# ============================================================

st.divider()

st.markdown(
    '<div class="section-label">04 / PARTNER PRODUCT INTELLIGENCE</div>',
    unsafe_allow_html=True,
)

st.subheader("Partner-Level Product Analysis")

if not partner_mix.empty:

    available_partners = (
        partner_mix[
            ["partner2Code", "partner2Desc", "partner_rank"]
        ]
        .drop_duplicates()
        .sort_values("partner_rank")
    )

    partner_options = (
        available_partners["partner2Code"].tolist()
    )

    partner_names = dict(
        zip(
            available_partners["partner2Code"],
            available_partners["partner2Desc"],
        )
    )

    selected_partner = st.selectbox(
        "Select an import partner",
        options=partner_options,
        format_func=lambda code: partner_names.get(
            code,
            str(code),
        ),
    )

    selected_mix = partner_mix[
        partner_mix["partner2Code"] == selected_partner
    ].copy()

    if not selected_mix.empty:

        selected_mix = selected_mix.sort_values(
            "product_rank_within_partner"
        )

        st.caption(
            f"Showing the leading products imported from "
            f"{partner_names.get(selected_partner, selected_partner)}."
        )

        partner_product_chart = make_partner_product_chart(
            selected_mix,
            f"Top Products Imported from "
            f"{partner_names.get(selected_partner, selected_partner)}",
        )

        show_chart(partner_product_chart)

        with st.expander("View detailed partner product data"):

            st.dataframe(
                selected_mix,
                width="stretch",
                hide_index=True,
            )

    else:

        st.info("No product mix data available for this partner.")

else:

    st.warning("Partner product mix data is unavailable.")


# ============================================================
# 14. METHODOLOGY AND DATA NOTES
# ============================================================

st.divider()

st.markdown(
    '<div class="section-label">05 / METHODOLOGY</div>',
    unsafe_allow_html=True,
)

st.subheader("Methodology & Data Notes")

with st.expander("Understanding the data and methodology"):

    st.markdown(
        """
        **Data source**

        UN Comtrade international merchandise trade statistics.

        **Coverage**

        - Reporter: India
        - Calendar year: 2023
        - Commodity classification: HS 2022
        - Commodity detail: Six-digit HS level

        **Valuation**

        - Exports are valued on an FOB basis.
        - Imports are valued on a CIF basis.
        - FOB and CIF use different valuation bases.

        **Partner interpretation**

        The partner2 field represents the secondary partner /
        country of consignment in the source data. It should not
        automatically be interpreted as the country of origin.

        **National and partner records**

        World aggregate records and partner-level records must
        not be added together because doing so can double-count
        trade.

        **Commodity quantities**

        Quantity units can differ between commodities.
        Therefore, quantities should not be summed across
        unrelated commodity codes without unit normalization.

        **Trend analysis**

        The current dashboard covers 2023 only. It does not
        calculate year-over-year growth or multi-year trends.

        **Trade valuation caveat**

        Export FOB values and import CIF values are not directly
        comparable valuation bases. Their difference should not
        be treated as a fully comparable economic trade balance.
        """
    )


# ============================================================
# 15. FOOTER
# ============================================================

st.markdown(
    """
    <div class="footer">
        TradeFlow | Global Trade Intelligence Platform
        <br>
        Built with Python · PySpark · Delta Lake · Parquet · Streamlit · Plotly
        <br><br>
        Data source: UN Comtrade | Reference period: 2023
    </div>
    """,
    unsafe_allow_html=True,
)
