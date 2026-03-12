from extract import load_data_from_csv
from transform import clean_clients, clean_instruments, clean_trades
from load import load_table
from helper import *
BASE_DIR = Path(__file__).parent.parent
INPUT_DIR = BASE_DIR / "data" / "input"

def run():
    logger.info("Pipeline started")
    client_file = INPUT_DIR / "clients.csv"
    instrument_file = INPUT_DIR / "instruments.csv"
    trade_files = sorted(INPUT_DIR.glob("trades_*.csv"))
    trade_file = trade_files[-1]
    # --- Extract ---
    try:
        clients_raw = load_data_from_csv(client_file)
        instruments_raw = load_data_from_csv(instrument_file)
        trades_raw = load_data_from_csv(trade_file)
    except Exception as e:
        logger.error(f"Failed load data from csv: {e}")
        raise

    # --- Transform ---
    try:
        data_clients = clean_clients(clients_raw)
        data_instruments = clean_instruments(instruments_raw)
        data_trades = clean_trades(trades_raw)
    except Exception as e:
        logger.error(f"Failed Transform data : {e}")
        raise

    # --- Master Data ---
    data_clients = merge_and_deduplicate(data_clients, "clients", "client_id")
    data_instruments = merge_and_deduplicate(data_instruments, "instruments", "instrument_id")

    # --- Data Validation ---
    valid_trades = data_trades[
        data_trades["instrument_id"].isin(data_instruments["instrument_id"]) &
        data_trades["client_id"].isin(data_clients["client_id"])
        ]

    #Load (save to database)
    try:
        load_table(data_clients, "clients",engine)
        load_table(data_instruments, "instruments",engine)
        load_table(valid_trades, "trades",engine)
    except Exception as e:
        logger.error(f"Failed loading data into database: {e}")
        raise

    logger.info("Pipeline completed successfully")


if __name__ == "__main__":
    run()

