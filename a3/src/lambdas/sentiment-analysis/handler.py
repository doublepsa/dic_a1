import json, os
from urllib.parse import unquote_plus

import boto3

ENDP = "http://localhost.localstack.cloud:4566" \
       if os.getenv("STAGE") == "local" else None

s3   = boto3.client("s3",  endpoint_url=ENDP)
ssm  = boto3.client("ssm", endpoint_url=ENDP)
dyna = boto3.resource("dynamodb", endpoint_url=ENDP)

sentiment_bucket = ssm.get_parameter(
    Name="/review-app/buckets/sentiment"
)["Parameter"]["Value"]

stats_table = dyna.Table(
    ssm.get_parameter(Name="/review-app/tables/sentiments")["Parameter"]["Value"]
)

POS = {"good", "great", "excellent", "love", "amazing", "perfect", "best"}
NEG = {"bad", "terrible", "awful", "hate", "worst", "poor", "boring"}


def label(text: str) -> str:
    words = text.lower().split()
    pos = sum(1 for w in words if w in POS)
    neg = sum(1 for w in words if w in NEG)
    if pos > neg:
        return "positive"
    if neg > pos:
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

        sentiment_key = f"sentiment_{key}"
        s3.put_object(
            Bucket=sentiment_bucket,
            Key=sentiment_key,
            Body=json.dumps(data),
            ContentType="application/json"
        )

    return {"analysed": len(event["Records"])}
