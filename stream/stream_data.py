import csv
import json
from datetime import datetime
from time import sleep

import boto3

BUCKET_NAME = "cricket-stream"
KEY = "test/1358412.csv"

STREAMING_PARTITION_KEY = "match_id"

s3 = boto3.client("s3", region_name="ap-southeast-2")
s3_resource = boto3.resource("s3", region_name="ap-southeast-2")
kinesis_client = boto3.client("kinesis", region_name="ap-southeast-2")

# Get kinesis stream names
streams = kinesis_client.list_streams().get("StreamNames")
kinesis_stream_name = (
    streams[0]
    if len(streams) == 1
    else Exception(f"Multiple Streams Found : {streams}")
)


def stream_data_simulator(stream_s3_bucket: str, stream_s3_key: str) -> None:
    """
    This function will read data in bucket and feed the file row by row to stream
    """
    s3_bucket = stream_s3_bucket
    s3_key = stream_s3_key

    # Read CSV Lines and split the file into lines
    csv_file = s3_resource.Object(s3_bucket, s3_key)
    s3_response = csv_file.get()
    lines = s3_response["Body"].read().decode("utf-8").split("\n")

    for row in csv.DictReader(lines):
        line_json = json.dumps(row)
        json_load = json.loads(line_json)

        json_load["timestamp"] = datetime.now().isoformat()
        match_id = json_load[STREAMING_PARTITION_KEY]

        # Write to Kinesis Streams:
        response = kinesis_client.put_record(
            StreamName=kinesis_stream_name,
            Data=json.dumps(json_load, indent=4),
            PartitionKey=str(match_id),
        )

        print(
            datetime.now(),
            " :: ",
            "HttpStatusCode:",
            response["ResponseMetadata"]["HTTPStatusCode"],
            ", ",
            json_load["striker"],
        )

        # Adding a temporary pause to stimulate time difference between balls
        sleep(1)


if __name__ == "__main__":
    stream_data_simulator(stream_s3_bucket=BUCKET_NAME, stream_s3_key=KEY)
