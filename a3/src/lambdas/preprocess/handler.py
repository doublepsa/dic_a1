import json
import os
import string
import typing
from urllib.parse import unquote_plus

import boto3

if typing.TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_ssm import SSMClient

# LocalStack endpoint in “local” stage
endpoint_url = None
if os.getenv("STAGE") == "local":
    endpoint_url = "http://localhost.localstack.cloud:4566"

s3: "S3Client" = boto3.client("s3", endpoint_url=endpoint_url)
ssm: "SSMClient" = boto3.client("ssm", endpoint_url=endpoint_url)


def get_processed_bucket_name() -> str:
    parameter = ssm.get_parameter(Name="/review-app/buckets/processed")
    return parameter["Parameter"]["Value"]


def basic_clean(text: str) -> str:
    text = text.lower()
    text = text.translate(str.maketrans("", "", string.punctuation))
    words = [w for w in text.split() if len(w) > 2]
    return " ".join(words)


def preprocess_review(review):
    return {
        "summary": basic_clean(review.get("summary", "")),
        "reviewText": basic_clean(review.get("reviewText", "")),
        "overall": str(review.get("overall", "")),
        "reviewerID": review.get("reviewerID", ""),
        "asin": review.get("asin", ""),
        "unixReviewTime": review.get("unixReviewTime", "")
    }


def handler(event, _context):
    processed_bucket = get_processed_bucket_name()

    for rec in event["Records"]:
        src_bucket = rec["s3"]["bucket"]["name"]
        key = unquote_plus(rec["s3"]["object"]["key"])

        obj = s3.get_object(Bucket=src_bucket, Key=key)
        clean = preprocess_review(json.loads(obj["Body"].read()))

        processed_key = f"processed_{key}"
        s3.put_object(
            Bucket=processed_bucket,
            Key=processed_key,
            Body=json.dumps(clean),
            ContentType="application/json"
        )

    return {"status": "ok", "processed": len(event["Records"])}
