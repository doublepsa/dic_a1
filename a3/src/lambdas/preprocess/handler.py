import json
import os
import string
import pathlib
from urllib.parse import unquote_plus
import traceback
import boto3
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import wordpunct_tokenize
from nltk.stem import WordNetLemmatizer

nltk.data.path.append(str(pathlib.Path(__file__).parent / "nltk_data"))

ENDPOINT_URL = "http://localhost.localstack.cloud:4566"

s3 = boto3.client("s3", endpoint_url=ENDPOINT_URL)
ssm = boto3.client("ssm", endpoint_url=ENDPOINT_URL)

stop_words = set(stopwords.words("english"))
lemmatizer = WordNetLemmatizer()


def clean_text(text: str) -> str:
    text = text.lower()
    text = text.translate(str.maketrans("", "", string.punctuation))
    tokens = [w for w in wordpunct_tokenize(text) if w.isalpha() and w not in stop_words]
    lemmas = [lemmatizer.lemmatize(tok) for tok in tokens]
    return " ".join(lemmas)


def preprocess_review(review):
    return {
        "summary": clean_text(review.get("summary", "")),
        "reviewText": clean_text(review.get("reviewText", "")),
        "overall": str(review.get("overall", "")),
        "reviewerID": review.get("reviewerID", ""),
        "asin": review.get("asin", ""),
        "unixReviewTime": review.get("unixReviewTime", "")
    }


def handler(event, _context):
    parameter = ssm.get_parameter(Name="/review-app/buckets/processed")
    processed_bucket = parameter["Parameter"]["Value"]

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
