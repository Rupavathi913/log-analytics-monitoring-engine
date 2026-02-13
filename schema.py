#schema files define the structure of data and type of the data , schema is nothing but role book of our data .
# uses of schema files :
#1. it ensures all rows follows the same structure and type of data 
#2. it helps in data validation and data cleaning .
#3. faster processing , easier analytics (flitering , mapping,grouping,anomaly detection becomes simple and faster)    
log_schema = {
"timestamp": "datetime",
"level": "string",
"service": "string",
"message": "string",
}
# dataframe=df.astype(log_schema)