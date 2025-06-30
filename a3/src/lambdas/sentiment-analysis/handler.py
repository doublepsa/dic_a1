import json, os
from urllib.parse import unquote_plus

import boto3
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer

ENDPOINT_URL = "http://localhost.localstack.cloud:4566"

s3 = boto3.client("s3", endpoint_url=ENDPOINT_URL)
ssm = boto3.client("ssm", endpoint_url=ENDPOINT_URL)
dynamodb = boto3.resource("dynamodb", endpoint_url=ENDPOINT_URL)

nltk.data.path.append(os.path.join(os.path.dirname(__file__), "nltk_data"))
sentimentAnalyzer = SentimentIntensityAnalyzer()


def label(text: str) -> str:
    score = sentimentAnalyzer.polarity_scores(text)["compound"]
    if score >= 0.05:
        return "positive"
    if score <= -0.05:
        return "negative"
    return "neutral"


def handler(event, _ctx):
    for rec in event["Records"]:
        src_bucket = rec["s3"]["bucket"]["name"]
        key = unquote_plus(rec["s3"]["object"]["key"])

        obj = s3.get_object(Bucket=src_bucket, Key=key)
        data = json.loads(obj["Body"].read())

        sentiment = label(f"{data.get('summary', '')} {data.get('reviewText', '')}")
        data["sentiment"] = sentiment

        parameter = ssm.get_parameter(Name="/review-app/tables/sentiments")
        stats_table = dynamodb.Table(parameter["Parameter"]["Value"])

        stats_table.update_item(
            Key={"sentiment": sentiment},
            UpdateExpression="ADD cnt :c",
            ExpressionAttributeValues={":c": 1}
        )

        parameter = ssm.get_parameter(Name="/review-app/buckets/sentiment")
        sentiment_bucket = parameter["Parameter"]["Value"]

        s3.put_object(
            Bucket=sentiment_bucket,
            Key=f"sentiment_{key}",
            Body=json.dumps(data),
            ContentType="application/json"
        )

    return {"analysed": len(event["Records"])}
