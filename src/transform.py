from helper import *


def clean_clients(df):
    before = len(df)
    df = df.drop_duplicates(subset=["client_id"])
    after = len(df)
    logger.info(f"Clients duplicates removed: {before - after}")
    return df

def clean_instruments(df):
    df = clean_strings(df)
    df = df.drop_duplicates(subset=["instrument_id"])
    return df


def clean_trades(df):

    before = len(df)
    if 'side' in df.columns:
        df['side'] = df['side'].astype(str).str.strip().str.lower()

    cols = ['quantity', 'price', 'fees']
    for col in cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(",", "", regex=False)
            )
            df[col] = pd.to_numeric(df[col], errors='coerce')

    df = df.drop_duplicates(subset=["trade_id"])
    after = len(df)

    logger.info(f"Trades duplicates removed: {before-after}")

    return df