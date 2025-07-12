from minio import Minio

client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

bucket = "warehouse-script"
if client.bucket_exists(bucket):
    print(f"{bucket} exists") 
else:
    print(f"{bucket} does not exist")
    print(f"Create bucket {bucket}")
    client.make_bucket(bucket)


jsonFile = "HW_Week1_FDE_HoaiTam/data/jsonfile.json"
csvFile = "HW_Week1_FDE_HoaiTam/data/movies.csv"
parquetFile = "HW_Week1_FDE_HoaiTam/data/table.parquet"

# WRITE 
destination_file_json = "my-test-file.json"
destination_file_csv = "my-test-file.csv"
destination_file_parquet = "my-test-file.parquet"

# JSON file

client.fput_object(bucket, destination_file_json, jsonFile,)
print(
    jsonFile, "successfully uploaded as object",
    destination_file_json, "to bucket", bucket,
)

# CSV file

client.fput_object(bucket, destination_file_csv, csvFile,)
print(
    csvFile, "successfully uploaded as object",
    destination_file_csv, "to bucket", bucket,
)

# Parquet file

client.fput_object(bucket, destination_file_parquet, parquetFile,)
print(
    parquetFile, "successfully uploaded as object",
    destination_file_parquet, "to bucket", bucket,
)

# READ
client.fget_object(bucket, destination_file_json, "HW_Week1_FDE_HoaiTam/data/output/json-output.json")
client.fget_object(bucket, destination_file_csv, "HW_Week1_FDE_HoaiTam/data/output/csv-output.csv")
client.fget_object(bucket, destination_file_parquet, "HW_Week1_FDE_HoaiTam/data/output/parquet-output.parquet")