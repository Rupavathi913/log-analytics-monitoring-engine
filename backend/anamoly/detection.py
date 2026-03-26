# #Here we detect anamolies from log data by comparing current errors and normal behavior using stastics 

# # Here we detect anomalies from log data by comparing current errors and normal behavior using statistics

# import dask.dataframe as dd

# def detect_anamoly(log_df, z_threshold=3):

#     log_df["timestamp"] = dd.to_datetime(log_df["timestamp"])

#     # filter error logs
#     error_logs = log_df[log_df["level"] == "ERROR"].copy()

#     # count error per minute
#     error_logs["minute"] = error_logs["timestamp"].dt.floor("min")

#     error_counts = (
#         error_logs.groupby("minute")
#         .size()
#         .to_frame("error_count")
#     )

#     # Normal behavior statistics
#     mean = error_counts["error_count"].mean().compute()
#     std = error_counts["error_count"].std().compute()

#     if std == 0:
#          result = error_counts.compute()
#          result["is_anomaly"] = True   # flag all as anomalies when std is 0
#          result["z_score"] = 0
#          return result

#     error_counts["anomaly_score"] = (error_counts["error_count"] - mean) / std
#     error_counts["is_anomaly"] = error_counts["anomaly_score"].abs() > z_threshold

#     return error_counts[error_counts["is_anomaly"]].compute()
# #threshold value, error count based on minute and time stamp and absolute value
# #in log level we have info,debug,errors and warning from log data in the part of levels
# #level :
# # 1. auth
# # 2. errors
# # 3. debug
# # 4. warnings
# #before filtering:
# #10:01 ERROR
# #10:01 INFO
# #10:01 ERROR
# #10:02 ERROR
# #10:02 ERROR
# #10:02 DEBUG
# #After filtering:
# #10:01 ERROR
# #10:01 ERROR
# #10:02 ERROR
# #10:02 ERROR


import dask.dataframe as dd
import numpy as np

def detect_anamoly(df, threshold=1.5):
    # Keep only ERROR logs
    error_df = df[df["level"] == "ERROR"].copy()

    # If no error logs, return empty dataframe
    if len(error_df) == 0:
        return error_df

    # Convert timestamp column to datetime
    error_df["timestamp"] = dd.to_datetime(error_df["timestamp"])

    # Create minute column (group by minute)
    error_df["minute"] = error_df["timestamp"].dt.floor("min")

    # Count errors per minute
    error_count = error_df.groupby("minute").size().reset_index(name="error_count")

    # Calculate mean and std
    mean = error_count["error_count"].mean().compute()
    std = error_count["error_count"].std().compute()

    # Handle std = 0 or NaN case
    if std == 0 or np.isnan(std):
        error_count["z_score"] = 0
    else:
        error_count["z_score"] = (error_count["error_count"] - mean) / std

    # Absolute z-score
    error_count["abs_z_score"] = abs(error_count["z_score"])

    # Detect anomalies
    anomalies = error_count[error_count["abs_z_score"] > threshold]

    return anamolies