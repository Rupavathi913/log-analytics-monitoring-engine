#why pipeline?
#this pipeline injects the raw log data using dask bag. parse them into structured data with the help of dask data frame, we can filter the invalid log entries and converts into data frame using meta data.

#why meta data?
#dask cannot guess the column type automatically , we tell dask what is key and what is the value(passing key and value pairs) like what are column names and data types of the particular column.

#why objects?
#raw log data are strings intially ,which are in text files, using objects it is safer browser for better understanding

# 

import dask.dataframe as dd
from backend.injection.parser import parse_log_line
from backend.injection.loader import load_logs

def process_pipeline(file_path):
    bag=load_logs(file_path)
    parsed = (
        bag.map(parse_log_line)
        .filter(lambda x: x is not None)  # Filter out lines that failed to parse
    )
    meta_data = {
        "timestamp": "datetime64[ns]", 
        "level" : "string", 
        "service" : "string", 
        "message" : "string",
    }
    df = parsed.to_dataframe(meta=meta_data)
    return df
