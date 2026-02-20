from datetime import time

from config.dask_config import start_dask
from ingestion.loader import load_logs
from processing.pipeline import build_pipeline
from processing.pipeline import build_pipeline


# def main():
#     # Create a Dask client
#     client = start_dask()
#     print(client)
#     print(f"Dashboard: {client.dashboard_link}")
    
#     # df = load_logs("data/sample_log.log")
    
#     start = time.time()
#     # df = build_pipeline("data/sample_log.log")
#     df = load_logs("data/sample_log.log")
#     total_logs = df.count().compute()
#     end = time.time()
    
#     # Now you can use the client to submit tasks to the Dask cluster
#     # For example, you can use client.submit() to run a function on the cluster
#     # result = client.submit(your_function, your_arguments)
#     input("Press Enter to stop the cluster...") # give this line in code
#     # Don't forget to close the client when you're done
#     client.close()  

def main():
    print("Starting Log Processing...")
    client = start_dask()
    print("Dask Started Successfully")

    df = load_logs("data/sample_log.log")
    print("Logs Loaded Successfully")

    print("\nFirst 5 Parsed Logs:")
    print(df.head())

    print("\nLog Count by Level:")
    result = df.count().compute()
    print(result)

    client.close()
    print("\nProcessing Finished Successfully!")
    
    
if __name__ == "__main__":
    main()
