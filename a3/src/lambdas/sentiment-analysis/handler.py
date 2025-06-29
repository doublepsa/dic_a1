import json, os
from urllib.parse import unquote_plus

import boto3
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer

# local-stack endpoint
ENDP = "http://localhost.localstack.cloud:4566" if os.getenv("STAGE") == "local" else None

s3   = boto3.client("s3",  endpoint_url=ENDP)
ssm  = boto3.client("ssm", endpoint_url=ENDP)
dyna = boto3.resource("dynamodb", endpoint_url=ENDP)

sentiment_bucket = ssm.get_parameter(
    Name="/review-app/buckets/sentiment"
)["Parameter"]["Value"]

stats_table = dyna.Table(
    ssm.get_parameter(Name="/review-app/tables/sentiments")["Parameter"]["Value"]
)

# make sure VADER can find its lexicon inside the layer/zip
nltk.data.path.append(os.path.join(os.path.dirname(__file__), "nltk_data"))
VADER = SentimentIntensityAnalyzer()

def label(text: str) -> str:
    score = VADER.polarity_scores(text)["compound"]
    if score >= 0.05:
        return "positive"
    if score <= -0.05:
        return "negative"
    return "neutral"

def handler(event, _ctx):
    for rec in event["Records"]:
        src_bucket = rec["s3"]["bucket"]["name"]
        key        = unquote_plus(rec["s3"]["object"]["key"])

        obj  = s3.get_object(Bucket=src_bucket, Key=key)
        data = json.loads(obj["Body"].read())

        senti = label(f"{data.get('summary','')} {data.get('reviewText','')}")
        data["sentiment"] = senti

        stats_table.update_item(
            Key={"sentiment": senti},
            UpdateExpression="ADD cnt :c",
            ExpressionAttributeValues={":c": 1}
        )

        s3.put_object(
            Bucket=sentiment_bucket,
            Key=f"sentiment_{key}",
            Body=json.dumps(data),
            ContentType="application/json"
        )

    return {"analysed": len(event["Records"])}
