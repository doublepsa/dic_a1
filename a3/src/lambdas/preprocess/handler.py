import json
import os
import string
import typing
import pathlib
from urllib.parse import unquote_plus
import traceback
import boto3
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import wordpunct_tokenize
from nltk.stem import WordNetLemmatizer

nltk.data.path.append(
    str(pathlib.Path(__file__).parent / "nltk_data")
)

if typing.TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_ssm import SSMClient

# LocalStack endpoint in “local” stage
endpoint_url = None
if os.getenv("STAGE") == "local":
    endpoint_url = "http://localhost.localstack.cloud:4566"

s3: "S3Client" = boto3.client("s3", endpoint_url=endpoint_url)
ssm: "SSMClient" = boto3.client("ssm", endpoint_url=endpoint_url)

stop_words = set(stopwords.words("english"))
lemmatizer  = WordNetLemmatizer()

def get_processed_bucket_name() -> str:
    parameter = ssm.get_parameter(Name="/review-app/buckets/processed")
    return parameter["Parameter"]["Value"]


def basic_clean(text: str) -> str:
    text = text.lower()
    text = text.translate(str.maketrans("", "", string.punctuation))
    tokens  = [w for w in wordpunct_tokenize(text) if w.isalpha() and w not in stop_words]
    lemmas  = [lemmatizer.lemmatize(tok) for tok in tokens]
    return " ".join(lemmas)

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
        s3.put_object(
            Bucket=processed_bucket,
            Key=f"processed_{key}",
            Body=json.dumps(clean),
            ContentType="application/json"
        )

    return {"status": "ok", "processed": len(event["Records"])}
