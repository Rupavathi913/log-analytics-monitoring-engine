from backend.config.dask_config import create_dask_client
from backend.pipeline.processing import process_pipeline
from backend.anamoly.detection import detect_anamoly
from backend.config.email_config import send_anomaly_email
import time


ADMIN_EMAIL = "admin@example.com" 


def main():
    client = create_dask_client()
    print(client)
    print(f"Dashboard link: {client.dashboard_link}")
    print("\n" + "=" * 50)

    start = time.time()

    # Build log processing pipeline
    log_df = process_pipeline("backend/sample_data/log_data.log")

    total_logs = log_df.count().compute()
    end = time.time()

    print("Total logs parsed:", total_logs)
    print("Time taken:", round(end - start, 2), "seconds")

    print("\n Running anamoly detection...")

    # Detect anomalies
    anamolies_df = detect_anamoly(log_df)

    # anomalies = anomalies_df.compute()
    anamolies = anamolies_df

    if anamolies.empty:
        print("No anamolies detected")
    else:
        print(f"🚨 {len(anamolies)} anamolies detected!")

        for _, row in anamolies.iterrows():
            anamoly_data = {
                "timestamp": row["timestamp"],
                "error_count": row["error_count"],
                "z_score": row["z_score"]
            }

            send_anamoly_email(
                to_email=ADMIN_EMAIL,
                anamoly=anamoly_data
            )

            print(
                f"📧 Alert sent | Time: {row['timestamp']} | "
                f"Errors: {row['error_count']}"
            )

    input("\nPress Enter to exit...")


if __name__ == "__main__":
    main()