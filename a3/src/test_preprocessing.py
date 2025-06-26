import json
import os
import boto3
import time

os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
os.environ["AWS_ACCESS_KEY_ID"] = "test"
os.environ["AWS_SECRET_ACCESS_KEY"] = "test"

s3 = boto3.client("s3", endpoint_url="http://localhost.localstack.cloud:4566")

def load_reviews(file_path):
    reviews = []
    with open(file_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line:  # skipping empty lines
                reviews.append(json.loads(line))
    return reviews

def upload_reviews_individually(reviews, bucket_name):
    print(f"Uploading {len(reviews)} reviews to {bucket_name}...")

    for i, review in enumerate(reviews):
        filename = f"review_{i:04d}_{review.get('reviewerID', 'unknown')}.json"

        s3.put_object(
            Bucket=bucket_name,
            Key=filename,
            Body=json.dumps(review),
            ContentType='application/json'
        )

        print(f"Uploaded {filename}")

        time.sleep(0.1)

def check_processed_results(processed_bucket):
    try:
        response = s3.list_objects_v2(Bucket=processed_bucket)
        if 'Contents' in response:
            print(f"Found {len(response['Contents'])} processed reviews in {processed_bucket}")
            for obj in response['Contents'][:5]:
                print(f"  - {obj['Key']}")
            if len(response['Contents']) > 5:
                print(f"  ... and {len(response['Contents']) - 5} more")
        else:
            print(f"No processed reviews found in {processed_bucket}")
    except Exception as e:
        print(f"Error checking processed bucket: {e}")

if __name__ == "__main__":
    reviews = load_reviews("../data/reviews_devset.json")
    print(f"Loaded {len(reviews)} reviews from dataset")

    input_bucket = "reviews-input"
    processed_bucket = "reviews-processed"

    upload_reviews_individually(reviews, input_bucket)

    print("Waiting 10 seconds for lambda processing...")
    time.sleep(10)

    check_processed_results(processed_bucket)