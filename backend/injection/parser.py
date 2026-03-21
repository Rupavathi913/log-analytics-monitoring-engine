#which is used to handle the data and reuseable code which is seperate from the dask 
#loading the data
#parsing the data
#structure the data and load the data using injection files
#parser: parser is a python method which is used to convert the raw data into structured data based on the schema which we have defined(translater)
#without parser:no columns are defined 
#no filtering is done
#no anamoloy is detection or no aggregation
#with parsing: we can filter error logs
#detect the unsual patterns
#it converts raw log data to the machine readable language

from datetime import datetime
import csv
import io

def parse_log_line(line):
    # Skip header row
    if line.startswith("timestamp"):
        return None

    try:
        row = next(csv.reader(io.StringIO(line.strip())))

        if len(row) != 4:
            return None

        timestamp_str, service, level, message = row

        return {
            "timestamp": datetime.fromisoformat(timestamp_str.split(".")[0]),
            "service": service,
            "level": level,
            "message": message,
        }

    except Exception:
        return None
# def parse_log_line(line):
#     #logic to matching log pattern
#     match=LOG_PATTERN.match(line) 
#     if not match: #if match is not found
#         return None
#     return {
#         "timestamp": match.group("timestamp"),
#         "level": match.group("level"),
#         "service": match.group("service"),
#         "message": match.group("message")
#     }


# #Groupdict()
#Strip()
#re.compile();recompile is a method in python which is used to create regex patterns
#python regex;python regular expression are used for searching matching and extract the data pattern from the given text
#r->raw data (log data)
#?->starting of pattern code
#P<timestamp>,P<level>,P<service>,P<message> which is used to capture the names of particular data
#\s -> used to remove white spaces between the name given



# groupdict() -> convert raw data into structure data (dictionary)
#     r'' -> raw Data
#     ?P<timestamp> -> named group
#     s+ -> for spaces one or more
#     ^ -> start of line
#     $ -> end of line
#     \S -> non space character
