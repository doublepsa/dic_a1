import json, os
from urllib.parse import unquote_plus

from better_profanity import profanity
import boto3

# load default english word list
profanity.load_censor_words()

ENDPOINT_URL = "http://localhost.localstack.cloud:4566"

s3 = boto3.client("s3", endpoint_url=ENDPOINT_URL)
ssm = boto3.client("ssm", endpoint_url=ENDPOINT_URL)
dynamodb = boto3.resource("dynamodb", endpoint_url=ENDPOINT_URL)


def handler(event, _ctx):
    for rec in event["Records"]:
        src_bucket = rec["s3"]["bucket"]["name"]
        key = unquote_plus(rec["s3"]["object"]["key"])

        obj = s3.get_object(Bucket=src_bucket, Key=key)
        data = json.loads(obj["Body"].read())

        tokens = data.get("summary_tokens", []) + data.get("reviewText_tokens", [])
        joined = " ".join(tokens)

        # fallback if tokens could not be created
        if not joined:
            summary = data.get("summary", "")
            review_text = data.get("reviewText", "")
            joined = summary + " " + review_text

        rude = profanity.contains_profanity(joined)

        reviewer = data.get("reviewerID", "unknown")

        parameter = ssm.get_parameter(Name="/review-app/tables/customers")
        customer_table = dynamodb.Table(parameter["Parameter"]["Value"])

        upd = customer_table.update_item(
            Key={"reviewerID": reviewer},
            UpdateExpression="ADD unpolite_count :inc",
            ExpressionAttributeValues={":inc": 1 if rude else 0},
            ReturnValues="UPDATED_NEW"
        )
        count = upd["Attributes"].get("unpolite_count", 0)

        # if the count is more than 3, we ban the customer
        banned = count > 3
        customer_table.update_item(
            Key={"reviewerID": reviewer},
            UpdateExpression="SET banned = :b",
            ExpressionAttributeValues={":b": banned}
        )

        data["containsBadWords"] = rude
        data["bannedAfterThis"] = banned

        parameter = ssm.get_parameter(Name="/review-app/buckets/profanity")
        profanity_bucket = parameter["Parameter"]["Value"]

        s3.put_object(
            Bucket=profanity_bucket,
            Key=f"profanity_{key}",
            Body=json.dumps(data),
            ContentType="application/json"
        )

    return {"scanned": len(event["Records"])}
