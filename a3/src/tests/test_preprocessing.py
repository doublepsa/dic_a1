import json, os, time, sys
import boto3
from pathlib import Path
from tqdm import tqdm

os.environ["AWS_ACCESS_KEY_ID"] = "test"
os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

ENDPOINT_URL = "http://localhost.localstack.cloud:4566"
UPLOAD_LIMIT = 2000

s3 = boto3.client("s3", endpoint_url=ENDPOINT_URL)
ssm = boto3.client("ssm", endpoint_url=ENDPOINT_URL)
dyna = boto3.resource("dynamodb", endpoint_url=ENDPOINT_URL)


def param(name: str) -> str:
    parameter = ssm.get_parameter(Name=name)
    return parameter["Parameter"]["Value"]


def load_reviews(file_path: str):
    reviews = []
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                reviews.append(json.loads(line))
    return reviews[:UPLOAD_LIMIT]



def upload_one_by_one(reviews: list[str], bucket):
    print(f"Uploading {len(reviews)} reviews to {bucket}")

    for idx, rev in tqdm(enumerate(reviews), total=min(200, len(reviews)), desc="Uploading"):
        key = f"review_{idx:04d}_{rev.get('reviewerID', 'unknown')}.json"
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(rev),
            ContentType="application/json",
        )
        time.sleep(0.25)

    print("Upload complete\n")


def wait_for_processing(expected, table_name):
    table = dyna.Table(table_name)
    while True:
        scan = table.scan(ProjectionExpression="cnt")
        total = sum(int(item["cnt"]) for item in scan.get("Items", []))
        if total >= expected:
            return
        time.sleep(1)


if __name__ == "__main__":
    DEVSET = Path(__file__).resolve().parent / "../../data/reviews_devset.json"
    reviews = load_reviews(str(DEVSET))
    print(f"Loaded {len(reviews)} reviews\n")

    input_bucket = param("/review-app/buckets/input")
    sentiments_tb = param("/review-app/tables/sentiments")
    customers_tb = param("/review-app/tables/customers")

    upload_one_by_one(reviews, input_bucket)

    print("Waiting for Lambdas to finish")
    wait_for_processing(len(reviews), sentiments_tb)

    sentiments_table = dyna.Table(sentiments_tb)
    sentiments_scan = sentiments_table.scan()

    sentiment_counts = {"positive": 0, "neutral": 0, "negative": 0}
    for item in sentiments_scan.get("Items", []):
        sentiment_counts[item["sentiment"]] = int(item["cnt"])

    customers_table = dyna.Table(customers_tb)
    customers_scan = customers_table.scan()

    unpolite_total = 0
    banned_users = []
    for item in customers_scan.get("Items", []):
        unpolite_total += int(item.get("unpolite_count", 0))
        if item.get("banned") is True:
            banned_users.append(item["reviewerID"])

    print(f"positive: {sentiment_counts['positive']}")
    print(f"neutral : {sentiment_counts['neutral']}")
    print(f"negative: {sentiment_counts['negative']}\n")

    print(f"Users with swearwords in reviews: {unpolite_total}\n")

    print(f"Banned users: {len(banned_users)}")
