import dask.dataframe as dd


def parse_log_line(file_path):

    print("Reading log file...")

    try:

        df = dd.read_csv(
            file_path,
            sep=" ",
            header=None,
            names=["Date", "Time", "Level", "Message"],
            engine="python"
        )

        print("Log file parsed successfully")

        return df

    except Exception as e:

        print("Error reading file:", e)

        return None