import io
import json
import os
import time

import boto3
import pandas as pd


S3_BUCKET_NAME = os.environ["S3_BUCKET_NAME"]
DYNAMODB_TABLE_NAME = os.environ["DYNAMODB_TABLE_NAME"]

s3_resource = boto3.resource("s3")
dynamodb_table = boto3.resource("dynamodb").Table(name=DYNAMODB_TABLE_NAME)


def lambda_handler(event, context) -> int:
    start_time = time.time()
    input_file = s3_resource.Object(bucket_name=S3_BUCKET_NAME, key=event["key"])
    with io.BytesIO(input_file.get()["Body"].read()) as in_memory_file:
        df = pd.read_csv(in_memory_file)

    with dynamodb_table.batch_writer() as writer:
        for i, row in df.iterrows():
            writer.put_item(Item=row.to_dict())
            if i % 10000 == 0:  # hard coded
                print(f"Wrote {i}th row")

    end_time = time.time()
    return int(end_time - start_time)
