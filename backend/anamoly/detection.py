#Here we detect anamolies from log data by comparing current errors and normal behavior using stastics 

import dask.dataframe as dd

def detect_anamoly(log_df,z_threshold=3):
    log_df["timestamp"] = dd.to_datetime(log_df["timestamp"])
    
    #count error per minute, per service
    error_logs = log_df[log_df["level"]=="ERROR"].copy()
    error_logs["minute"]  = error_logs["timestamp"].dt.floor("min")
    error_counts = (
        error_logs.groupby("minute").size()
    )
    #Normal behavior
    #suppose assume mean value as 20 the value will be stored in the key named error_count
    mean = error_counts["error_count"].compute().mean()
    std = error_counts["error_count"].compute().std()
    if std == 0:
        return error_counts.compute()
    error_counts["anomaly_score"] = (error_counts["error_count"] - mean) / std
    error_counts["is_anomaly"] = error_counts["anomaly_score"].abs() > z_threshold
    return error_counts[error_counts["is_anomaly"]].compute()
#threshold value, error count based on minute and time stamp and absolute value
#in log level we have info,debug,errors and warning from log data in the part of levels
#level :
# 1. auth
# 2. errors
# 3. debug
# 4. warnings
#before filtering:
#10:01 ERROR
#10:01 INFO
#10:01 ERROR
#10:02 ERROR
#10:02 ERROR
#10:02 DEBUG
#After filtering:
#10:01 ERROR
#10:01 ERROR
#10:02 ERROR
#10:02 ERROR