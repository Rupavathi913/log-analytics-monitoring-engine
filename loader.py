import dask.bag as db
from ingestion.parser import parse_log_line
from schema.schema import LOG_SCHEMA

def load_logs(file_path):
    bag = db.read_text(file_path)
    parsed = (
        bag.map(parse_log_line)
        .filter(lambda x: x is not None)  # Filter out lines that failed to parse
    )
    df = parsed.to_dataframe()
    return df.astype(LOG_SCHEMA)