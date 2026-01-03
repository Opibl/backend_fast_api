from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from pathlib import Path
import os
from dotenv import load_dotenv

# ======================================================
# LOAD ENV
# ======================================================

load_dotenv()

APP_NAME = os.getenv("APP_NAME", "FastAPI App")
CSV_PATH = os.getenv("CSV_PATH", "data/Superstore.csv")
CORS_ORIGINS = os.getenv("CORS_ORIGINS", "*").split(",")

# ======================================================
# APP
# ======================================================

app = FastAPI(title=APP_NAME)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS if CORS_ORIGINS != ["*"] else ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ======================================================
# LOAD DATASET
# ======================================================

BASE_DIR = Path(__file__).resolve().parent
DATA_PATH = BASE_DIR / CSV_PATH

if not DATA_PATH.exists():
    raise FileNotFoundError(f"Dataset not found at {DATA_PATH}")

df = pd.read_csv(
    DATA_PATH,
    encoding="latin1",
    engine="python",
    on_bad_lines="skip"
)

# ======================================================
# CLEAN DATA
# ======================================================

for col in ["Sales", "Profit"]:
    df[col] = (
        df[col]
        .astype(str)
        .str.replace("$", "", regex=False)
        .str.replace(",", "", regex=False)
        .astype(float)
    )

df["Order Date"] = pd.to_datetime(df["Order Date"], errors="coerce")
df = df.dropna(subset=["Order Date"])

# ======================================================
# API ENDPOINTS
# ======================================================

@app.get("/")
def root():
    return {"message": f"{APP_NAME} running 🚀"}

# ---------------- KPIS ----------------

@app.get("/kpis")
def get_kpis():
    total_sales = df["Sales"].sum()
    total_profit = df["Profit"].sum()
    orders = len(df)

    margin = (total_profit / total_sales) * 100 if total_sales > 0 else 0
    average_ticket = total_sales / orders if orders > 0 else 0

    return {
        "totalSales": round(total_sales, 2),
        "totalProfit": round(total_profit, 2),
        "orders": orders,
        "margin": round(min(margin, 100), 2),
        "averageTicket": round(average_ticket, 2),
    }

# ---------------- SALES BY MONTH ----------------

@app.get("/sales-by-month")
def sales_by_month():
    result = (
        df.groupby(df["Order Date"].dt.month)
        .agg(sales=("Sales", "sum"))
        .reset_index()
        .sort_values("Order Date")
    )

    return [
        {
            "month": pd.to_datetime(f"2020-{int(r['Order Date'])}-01").strftime("%b"),
            "sales": round(r["sales"], 2),
        }
        for _, r in result.iterrows()
    ]

# ---------------- SALES BY YEAR ----------------

@app.get("/sales-by-year")
def sales_by_year():
    result = (
        df.groupby(df["Order Date"].dt.year)
        .agg(sales=("Sales", "sum"), profit=("Profit", "sum"))
        .reset_index()
        .sort_values("Order Date")
    )

    return [
        {
            "year": int(r["Order Date"]),
            "sales": round(r["sales"], 2),
            "profit": round(r["profit"], 2),
        }
        for _, r in result.iterrows()
    ]

# ---------------- SALES BY CATEGORY ----------------

@app.get("/sales-by-category")
def sales_by_category():
    return (
        df.groupby("Category")
        .agg(sales=("Sales", "sum"), profit=("Profit", "sum"))
        .reset_index()
        .round(2)
        .to_dict("records")
    )

# ---------------- KPIS BY CATEGORY ----------------

@app.get("/kpis-by-category")
def kpis_by_category():
    result = (
        df.groupby("Category")
        .agg(
            sales=("Sales", "sum"),
            profit=("Profit", "sum"),
            orders=("Sales", "count"),
        )
        .reset_index()
    )

    result["margin"] = (result["profit"] / result["sales"]) * 100
    return result.round(2).to_dict("records")

# ---------------- SALES BY REGION ----------------

@app.get("/sales-by-region")
def sales_by_region():
    return (
        df.groupby("Region")
        .agg(sales=("Sales", "sum"), profit=("Profit", "sum"))
        .reset_index()
        .round(2)
        .to_dict("records")
    )

# ---------------- SALES BY STATE ----------------

@app.get("/sales-by-state")
def sales_by_state():
    return (
        df.groupby("State")
        .agg(sales=("Sales", "sum"))
        .reset_index()
        .sort_values("sales", ascending=False)
        .round(2)
        .to_dict("records")
    )

# ---------------- TOP PRODUCTS ----------------

@app.get("/top-products")
def top_products(limit: int = 10):
    return (
        df.groupby("Product Name")
        .agg(sales=("Sales", "sum"), profit=("Profit", "sum"))
        .reset_index()
        .sort_values("sales", ascending=False)
        .head(limit)
        .round(2)
        .to_dict("records")
    )
