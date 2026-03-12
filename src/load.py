from helper import *

def load_table(df: pd.DataFrame, table_name: str, engine):

    if df.empty:
        logger.warning(f"No data to load into {table_name}. Skipping.")
        return

    try:
        with engine.begin() as conn:

            if table_name == "trades":

                trade_date = pd.to_datetime(df["trade_time"]).dt.date.iloc[0]
                conn.execute(
                    text("""
                                        DELETE FROM trades
                                        WHERE DATE(trade_time) = :trade_date
                                    """),
                    {"trade_date": str(trade_date)}
                )

                logger.info(f"Deleted existing trades for date: {trade_date}")

                df.to_sql(table_name, con=conn, if_exists="append", index=False)

            # reference tables
            else:
                df.to_sql(table_name, con=conn, if_exists="replace", index=False)

        logger.info(f"Loaded {len(df)} rows into '{table_name}'")

    except Exception as e:
        logger.error(f"Failed to load table {table_name}: {e}")
        raise