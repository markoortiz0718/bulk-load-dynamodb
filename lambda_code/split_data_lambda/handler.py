import io
import json
import os

import boto3
import pandas as pd


S3_BUCKET_NAME = os.environ["S3_BUCKET_NAME"]
PARTITION_ROW_COUNT = json.loads(os.environ["PARTITION_ROW_COUNT"])
s3_resource = boto3.resource("s3")


def lambda_handler(event, context) -> list[str]:
    input_file = s3_resource.Object(bucket_name=S3_BUCKET_NAME, key=event["key"])
    with io.BytesIO(input_file.get()["Body"].read()) as in_memory_file:
        df = pd.read_csv(in_memory_file)

    partition_files = []
    for i, start_index in enumerate(range(0, len(df), PARTITION_ROW_COUNT)):
        df_partition = df[start_index : start_index + PARTITION_ROW_COUNT]
        partition_file = f"partitioned/{i}.csv"
        output_file = s3_resource.Object(bucket_name=S3_BUCKET_NAME, key=partition_file)
        output_file.put(Body=df_partition.to_csv(index=False).encode("utf-8"))
        print(
            f"Wrote partition {i} with {PARTITION_ROW_COUNT} rows to "
            f"s3://{S3_BUCKET_NAME}/{partition_file}"
        )
        partition_files.append(partition_file)
    return partition_files
