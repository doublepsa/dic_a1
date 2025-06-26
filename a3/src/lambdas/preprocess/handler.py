import json
import os
import typing
from urllib.parse import unquote_plus

import boto3

if typing.TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_ssm import SSMClient

endpoint_url = None
if os.getenv("STAGE") == "local":
    endpoint_url = "https://localhost.localstack.cloud:4566"

s3: "S3Client" = boto3.client("s3", endpoint_url=endpoint_url)
ssm: "SSMClient" = boto3.client("ssm", endpoint_url=endpoint_url)


def get_processed_bucket_name() -> str:
    parameter = ssm.get_parameter(Name="/review-app/buckets/processed")
    return parameter["Parameter"]["Value"]


def preprocess_review(review_data):
    summary = review_data.get("summary", "")
    review_text = review_data.get("reviewText", "")
    overall = review_data.get("overall", "")

    # Basic cleaning for now
    processed = {
        "summary": summary.strip(),
        "reviewText": review_text.strip(),
        "overall": str(overall).strip(),
        "reviewerID": review_data.get("reviewerID", ""),
        "asin": review_data.get("asin", ""),
        "unixReviewTime": review_data.get("unixReviewTime", "")
    }

    return processed


def handler(event, context):
    processed_bucket = get_processed_bucket_name()

    for record in event["Records"]:
        source_bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])
        print(f"Processing {source_bucket}/{key}")

        response = s3.get_object(Bucket=source_bucket, Key=key)
        review_data = json.loads(response['Body'].read().decode('utf-8'))

        processed_review = preprocess_review(review_data)

        processed_key = f"processed_{key}"
        s3.put_object(
            Bucket=processed_bucket,
            Key=processed_key,
            Body=json.dumps(processed_review),
            ContentType='application/json'
        )

        print(f"Processed review saved to {processed_bucket}/{processed_key}")