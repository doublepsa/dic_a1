import json, os, time, sys
import boto3
from pathlib import Path
from tqdm import tqdm

os.environ["AWS_ACCESS_KEY_ID"] = "test"
os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

ENDPOINT = "http://localhost.localstack.cloud:4566"

s3 = boto3.client("s3", endpoint_url=ENDPOINT)
ssm = boto3.client("ssm", endpoint_url=ENDPOINT)
dyna = boto3.resource("dynamodb", endpoint_url=ENDPOINT)


def param(name: str) -> str:
    return ssm.get_parameter(Name=name)["Parameter"]["Value"]


def load_reviews(file_path: str):
    reviews = []
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                reviews.append(json.loads(line))
    return reviews


def upload_one_by_one(reviews, bucket):
    print(f"Uploading {len(reviews)} reviews → {bucket}")
    for idx, rev in enumerate(reviews):
        key = f"review_{idx:04d}_{rev.get('reviewerID', 'na')}.json"
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(rev),
            ContentType="application/json",
        )
    print("Upload complete.\n")


def wait_for_processing(expected, table_name):
    table = dyna.Table(table_name)
    for _ in range(60):
        scan = table.scan(ProjectionExpression="cnt")
        total = sum(int(item["cnt"]) for item in scan.get("Items", []))
        if total >= expected:
            return
        time.sleep(1)
    print("⚠  Timeout: pipeline did not finish in 60 s", file=sys.stderr)


if __name__ == "__main__":
    DEVSET = Path(__file__).resolve().parent / "../../data/reviews_devset.json"
    reviews = load_reviews(str(DEVSET))
    print(f"Loaded {len(reviews)} reviews\n")

    input_bucket = param("/review-app/buckets/input")
    sentiments_tb = param("/review-app/tables/sentiments")
    customers_tb = param("/review-app/tables/customers")

    upload_one_by_one(reviews, input_bucket)

    print("Waiting for Lambdas to finish …")
    wait_for_processing(len(reviews), sentiments_tb)

    sentiments_table = dyna.Table(sentiments_tb)
    sentiments_scan = sentiments_table.scan()

    sentiment_counts = {"positive": 0, "neutral": 0, "negative": 0}
    for item in sentiments_scan.get("Items", []):
        sentiment_counts[item["sentiment"]] = int(item["cnt"])

    customers_table = dyna.Table(customers_tb)
    cust_scan = customers_table.scan()

    unpolite_total = 0
    banned_users = []
    for item in cust_scan.get("Items", []):
        unpolite_total += int(item.get("unpolite_count", 0))
        if item.get("banned") is True:
            banned_users.append(item["reviewerID"])

    print(f"Sentiment distribution (devset only):")
    print(f"  positive: {sentiment_counts['positive']}")
    print(f"  neutral : {sentiment_counts['neutral']}")
    print(f"  negative: {sentiment_counts['negative']}\n")

    print(f"Total reviews that failed profanity check: {unpolite_total}\n")

    if banned_users:
        print("Banned users:")
        for u in banned_users:
            print(f"  • {u}")
    else:
        print("No users were banned.")
