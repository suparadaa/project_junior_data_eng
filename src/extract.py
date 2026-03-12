from helper import *
INPUT_PATH = Path("data/input")


def load_data_from_csv(file_name):
    df = pd.read_csv(file_name)

    logger.info(f"Loaded {file_name}: {len(df)} rows")

    return df