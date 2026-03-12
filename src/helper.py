import pandas as pd
import logging
from pathlib import Path
import glob
from sqlalchemy import create_engine,text

logger = logging.getLogger(__name__)
DB_URI = "postgresql://postgres:postgres@db:5432/brokerage"

engine = create_engine(DB_URI)


def clean_strings(df):
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip().str.lower()
    return df


def clean_numeric(series):
    return (
        series.astype(str)
        .str.replace(",", "")
        .astype(float)
    )

def merge_and_deduplicate(df_new, table_name, key_col):
    try:
        query = f"SELECT * FROM {table_name}"
        data_db = pd.read_sql(query, engine)
        combined = pd.concat([data_db, df_new], ignore_index=True)
    except Exception:
        # ถ้ารันครั้งแรก ตารางยังไม่มีใน DB โค้ดจะเข้าเงื่อนไขนี้
        logger.info(f"Table '{table_name}' does not exist yet. First run detected.")
        combined = df_new

    # ถ้ามี id ซ้ำ ให้เก็บตัวหลัง (csv ล่าสุด)
    combined = combined.drop_duplicates(subset=[key_col], keep="last")
    return combined
