import json, os
from urllib.parse import unquote_plus

from better_profanity import profanity
import boto3

profanity.load_censor_words()  # loads default English word-list

ENDP = "http://localhost.localstack.cloud:4566" \
    if os.getenv("STAGE") == "local" else None

s3 = boto3.client("s3", endpoint_url=ENDP)
ssm = boto3.client("ssm", endpoint_url=ENDP)
dyna = boto3.resource("dynamodb", endpoint_url=ENDP)

profanity_bucket = ssm.get_parameter(
    Name="/review-app/buckets/profanity"
)["Parameter"]["Value"]

customer_table = dyna.Table(
    ssm.get_parameter(Name="/review-app/tables/customers")["Parameter"]["Value"]
)


def contains_bad_words(text: str) -> bool:
    words = text.lower().split()
    return any(w in BAD_WORDS for w in words)


def handler(event, _ctx):
    for rec in event["Records"]:
        src_bucket = rec["s3"]["bucket"]["name"]
        key = unquote_plus(rec["s3"]["object"]["key"])

        obj = s3.get_object(Bucket=src_bucket, Key=key)
        data = json.loads(obj["Body"].read())

        tokens = data.get("summary_tokens", []) + data.get("reviewText_tokens", [])
        joined = " ".join(tokens) or f"{data.get('summary', '')} {data.get('reviewText', '')}"

        rude = profanity.contains_profanity(joined)

        reviewer = data.get("reviewerID", "unknown")
        upd = customer_table.update_item(
            Key={"reviewerID": reviewer},
            UpdateExpression="ADD unpolite_count :inc",
            ExpressionAttributeValues={":inc": 1 if rude else 0},
            ReturnValues="UPDATED_NEW"
        )
        count = upd["Attributes"].get("unpolite_count", 0)
        banned = count > 3
        customer_table.update_item(
            Key={"reviewerID": reviewer},
            UpdateExpression="SET banned = :b",
            ExpressionAttributeValues={":b": banned}
        )

        data["containsBadWords"] = rude
        data["bannedAfterThis"] = banned

        s3.put_object(
            Bucket=profanity_bucket,
            Key=f"profanity_{key}",
            Body=json.dumps(data),
            ContentType="application/json"
        )

    return {"scanned": len(event["Records"])}
