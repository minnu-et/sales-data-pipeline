"""
YouTube DE Project — Analytics Dashboard
Reads Gold layer data directly from S3
"""

import streamlit as st
import pandas as pd
import boto3
from io import BytesIO
import pyarrow.parquet as pq
import s3fs

# ============================================================
# CONFIG
# ============================================================
BUCKET = "de-project-s3-bucket-minnu"
GOLD_SALES_PATH = "gold/sales_enriched"
GOLD_CUSTOMER_METRICS_PATH = "gold/customer_metrics"

# ============================================================
# PAGE CONFIG
# ============================================================
st.set_page_config(
    page_title="Sales Analytics Dashboard",
    page_icon="📊",
    layout="wide"
)

# ============================================================
# CUSTOM CSS
# ============================================================
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;700&display=swap');

    html, body, [class*="css"] {
        font-family: 'DM Sans', sans-serif;
    }

    .kpi-card {
        background: linear-gradient(135deg, #1e293b, #334155);
        border-radius: 12px;
        padding: 24px;
        text-align: center;
        color: white;
        border: 1px solid #475569;
    }
    .kpi-value {
        font-size: 1.6rem;
        font-weight: 700;
        margin: 8px 0 4px 0;
        color: #38bdf8;
        white-space: nowrap;
    }
    .kpi-label {
        font-size: 0.85rem;
        text-transform: uppercase;
        letter-spacing: 1.5px;
        color: #94a3b8;
    }
    .section-header {
        font-size: 1.1rem;
        font-weight: 700;
        color: #e2e8f0;
        margin: 32px 0 16px 0;
        padding-bottom: 8px;
        border-bottom: 2px solid #38bdf8;
        display: inline-block;
    }
    .stApp {
        background-color: #0f172a;
    }
    h1 {
        color: #f1f5f9 !important;
    }
</style>
""", unsafe_allow_html=True)


# ============================================================
# DATA LOADING
# ============================================================
@st.cache_data(ttl=300)
def load_s3_parquet(prefix: str) -> pd.DataFrame:
    """Load parquet files from S3 prefix (handles partitioned data)."""
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    dfs = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet"):
                response = s3.get_object(Bucket=BUCKET, Key=key)
                df = pd.read_parquet(BytesIO(response["Body"].read()))

                # Extract partition columns from path
                for part in key.split("/"):
                    if "=" in part:
                        col_name, col_value = part.split("=", 1)
                        df[col_name] = int(
                            col_value) if col_value.isdigit() else col_value

                dfs.append(df)

    if dfs:
        return pd.concat(dfs, ignore_index=True)

    st.error(f"No parquet files found at {prefix}")
    return pd.DataFrame()


# ============================================================
# LOAD DATA
# ============================================================
st.title("📊 Sales Analytics Dashboard")
st.caption(
    "Real-time insights from the Gold layer  •  Data Engineering Portfolio Project")

with st.spinner("Loading data from S3..."):
    sales_df = load_s3_parquet(GOLD_SALES_PATH)
    customer_df = load_s3_parquet(GOLD_CUSTOMER_METRICS_PATH)

if sales_df.empty:
    st.error("No sales data found in S3. Run the pipeline first!")
    st.stop()

# ============================================================
# KPI CARDS
# ============================================================
total_revenue = sales_df["total_cost"].sum()
total_orders = len(sales_df)
total_customers = sales_df["customer_id"].nunique()
avg_order_value = sales_df["total_cost"].mean()
total_products = sales_df["product_id"].nunique()
total_stores = sales_df["store_id"].nunique()

cols = st.columns(6)
kpis = [
    ("Total Revenue", f"₹{total_revenue/100000:,.1f}L"),
    ("Total Orders", f"{total_orders:,}"),
    ("Customers", f"{total_customers:,}"),
    ("Avg Order Value", f"₹{avg_order_value:,.0f}"),
    ("Products", f"{total_products:,}"),
    ("Stores", f"{total_stores:,}"),
]

for col, (label, value) in zip(cols, kpis):
    col.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">{label}</div>
            <div class="kpi-value">{value}</div>
        </div>
    """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ============================================================
# FILTERS (Sidebar)
# ============================================================
with st.sidebar:
    st.header("🔍 Filters")

    # State filter
    all_states = sorted(sales_df["customer_state"].dropna().unique())
    selected_states = st.multiselect(
        "Customer State", all_states, default=all_states)

    # Category filter
    all_categories = sorted(sales_df["category"].dropna().unique())
    selected_categories = st.multiselect(
        "Product Category", all_categories, default=all_categories)

    # Brand filter
    all_brands = sorted(sales_df["brand"].dropna().unique())
    selected_brands = st.multiselect("Brand", all_brands, default=all_brands)

# Apply filters
filtered_df = sales_df[
    (sales_df["customer_state"].isin(selected_states)) &
    (sales_df["category"].isin(selected_categories)) &
    (sales_df["brand"].isin(selected_brands))
]

# ============================================================
# ROW 1: Monthly Sales Trend + Sales by Category
# ============================================================
left, right = st.columns(2)

with left:
    st.markdown('<div class="section-header">Top 10 Products by Revenue</div>', unsafe_allow_html=True)
    top_products = (
        filtered_df.groupby("product_name")["total_cost"]
        .sum()
        .sort_values(ascending=True)
        .tail(10)
        .reset_index()
    )
    st.bar_chart(top_products.set_index("product_name"), use_container_width=True)

with right:
    st.markdown('<div class="section-header">Revenue by Category</div>',
                unsafe_allow_html=True)
    cat_rev = (
        filtered_df.groupby("category")["total_cost"]
        .sum()
        .sort_values(ascending=True)
        .reset_index()
    )
    st.bar_chart(cat_rev.set_index("category"), use_container_width=True)

# ============================================================
# ROW 2: Top 10 Customers + Sales by Store
# ============================================================
left2, right2 = st.columns(2)

with left2:
    st.markdown('<div class="section-header">Top 10 Customers by Revenue</div>',
                unsafe_allow_html=True)
    if not customer_df.empty:
        top_customers = (
            customer_df
            .nlargest(10, "total_spent")
            [["first_name", "last_name", "city", "total_orders",
                "total_spent", "avg_order_value"]]
            .reset_index(drop=True)
        )
        top_customers["customer"] = top_customers["first_name"] + \
            " " + top_customers["last_name"]
        top_customers["total_spent"] = top_customers["total_spent"].apply(
            lambda x: f"₹{x:,.0f}")
        top_customers["avg_order_value"] = top_customers["avg_order_value"].apply(
            lambda x: f"₹{x:,.0f}")
        st.dataframe(
            top_customers[["customer", "city", "total_orders",
                           "total_spent", "avg_order_value"]],
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("No customer metrics data available")

with right2:
    st.markdown('<div class="section-header">Revenue by Store</div>',
                unsafe_allow_html=True)
    store_rev = (
        filtered_df.groupby("store_name")["total_cost"]
        .sum()
        .sort_values(ascending=True)
        .reset_index()
    )
    st.bar_chart(store_rev.set_index("store_name"), use_container_width=True)

# ============================================================
# ROW 3: Brand Performance + State Distribution
# ============================================================
left3, right3 = st.columns(2)

with left3:
    st.markdown('<div class="section-header">Revenue by Brand</div>',
                unsafe_allow_html=True)
    brand_rev = (
        filtered_df.groupby("brand")["total_cost"]
        .sum()
        .sort_values(ascending=False)
        .head(10)
        .reset_index()
    )
    st.bar_chart(brand_rev.set_index("brand"), use_container_width=True)

with right3:
    st.markdown('<div class="section-header">Revenue by State</div>',
                unsafe_allow_html=True)
    state_rev = (
        filtered_df.groupby("customer_state")["total_cost"]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
    )
    st.bar_chart(state_rev.set_index("customer_state"),
                 use_container_width=True)

# ============================================================
# FOOTER
# ============================================================
st.markdown("---")
st.markdown(
    "<div style='text-align:center; color:#64748b; font-size:0.8rem;'>"
    "Data Engineering Portfolio Project • Bronze → Silver → Gold Pipeline • "
    f"Showing {len(filtered_df):,} of {len(sales_df):,} records"
    "</div>",
    unsafe_allow_html=True
)
