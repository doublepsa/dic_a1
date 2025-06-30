#!/usr/bin/env bash

# This scripts fails on error but for specific commands we want to ignore errors
# we use '|| true' to ignore the error
set -e

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
LAMBDA_DIR="$SCRIPT_DIR/lambdas"

# Create S3 buckets
awslocal s3 mb "s3://reviews-input" || true
awslocal s3 mb "s3://reviews-processed" || true
awslocal s3 mb "s3://reviews-profanity" || true
awslocal s3 mb "s3://reviews-sentiment" || true

# Create SSM parameters
awslocal ssm put-parameter --name "/review-app/buckets/input" --value "reviews-input" --type String --overwrite
awslocal ssm put-parameter --name "/review-app/buckets/processed" --value "reviews-processed" --type String --overwrite
awslocal ssm put-parameter --name "/review-app/buckets/profanity" --value "reviews-profanity" --type String --overwrite
awslocal ssm put-parameter --name "/review-app/buckets/sentiment" --value "reviews-sentiment" --type String --overwrite

# Create DynamoDB tables
awslocal dynamodb create-table \
  --table-name customers \
  --attribute-definitions AttributeName=reviewerID,AttributeType=S \
  --key-schema AttributeName=reviewerID,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST || true

awslocal dynamodb create-table \
  --table-name sentiments \
  --attribute-definitions AttributeName=sentiment,AttributeType=S \
  --key-schema AttributeName=sentiment,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST || true

awslocal ssm put-parameter --name "/review-app/tables/customers"  --value "customers"  --type String --overwrite
awslocal ssm put-parameter --name "/review-app/tables/sentiments" --value "sentiments" --type String --overwrite

# Deploy preprocess lambda
cd "$LAMBDA_DIR/preprocess"
rm -rf package preprocess.zip
mkdir package
pip install -r requirements.txt -t package --platform manylinux2014_x86_64 --only-binary=:all:
python -m nltk.downloader stopwords wordnet -d package/nltk_data
zip preprocess.zip handler.py
cd package
zip -r ../preprocess.zip *
cd - >/dev/null
awslocal lambda create-function \
  --function-name preprocess \
  --runtime python3.11 \
  --role arn:aws:iam::000000000000:role/lambda-role \
  --handler handler.handler \
  --zip-file "fileb://$LAMBDA_DIR/preprocess/preprocess.zip" \
  --environment Variables='{STAGE=local}' \
  --timeout 15 \
|| true

# Deploy profanity-check lambda
cd "$LAMBDA_DIR/profanity-check"
rm -rf package profanity-check.zip
mkdir package
pip install -r requirements.txt -t package --platform manylinux2014_x86_64 --only-binary=:all:
zip profanity-check.zip handler.py
cd package
zip -r ../profanity-check.zip *
cd - >/dev/null
awslocal lambda create-function \
  --function-name profanity-check \
  --runtime python3.11 \
  --role arn:aws:iam::000000000000:role/lambda-role \
  --handler handler.handler \
  --memory-size 512 \
  --timeout 30 \
  --zip-file "fileb://$LAMBDA_DIR/profanity-check/profanity-check.zip" \
  --environment Variables='{STAGE=local}' \
|| true

# Deploy sentiment-analysis lambda
cd "$LAMBDA_DIR/sentiment-analysis"
rm -rf package sentiment-analysis.zip
mkdir package
pip install -r requirements.txt -t package --platform manylinux2014_x86_64 --only-binary=:all:
python -m nltk.downloader vader_lexicon -d package/nltk_data
zip sentiment-analysis.zip handler.py
cd package
zip -r ../sentiment-analysis.zip *
cd - >/dev/null
awslocal lambda create-function \
  --function-name sentiment-analysis \
  --runtime python3.11 \
  --role arn:aws:iam::000000000000:role/lambda-role \
  --handler handler.handler \
  --memory-size 512 \
  --timeout 30 \
  --zip-file "fileb://$LAMBDA_DIR/sentiment-analysis/sentiment-analysis.zip" \
  --environment Variables='{STAGE=local}' \
|| true

# Add lambda permissions
awslocal lambda add-permission \
  --function-name preprocess \
  --statement-id s3invoke1 \
  --action lambda:InvokeFunction \
  --principal s3.amazonaws.com \
  --source-arn arn:aws:s3:::reviews-input || true

awslocal lambda add-permission \
  --function-name profanity-check \
  --statement-id s3invoke2 \
  --action lambda:InvokeFunction \
  --principal s3.amazonaws.com \
  --source-arn arn:aws:s3:::reviews-processed || true

awslocal lambda add-permission \
  --function-name sentiment-analysis \
  --statement-id s3invoke3 \
  --action lambda:InvokeFunction \
  --principal s3.amazonaws.com \
  --source-arn arn:aws:s3:::reviews-profanity || true

# Configure S3 bucket notifications
awslocal s3api put-bucket-notification-configuration \
  --bucket reviews-input \
  --notification-configuration '{
    "LambdaFunctionConfigurations": [
      {
        "Id": "prep",
        "LambdaFunctionArn": "arn:aws:lambda:us-east-1:000000000000:function:preprocess",
        "Events": ["s3:ObjectCreated:*"]
      }
    ]
  }'

awslocal s3api put-bucket-notification-configuration \
  --bucket reviews-processed \
  --notification-configuration '{
    "LambdaFunctionConfigurations": [
      {
        "Id": "prof",
        "LambdaFunctionArn": "arn:aws:lambda:us-east-1:000000000000:function:profanity-check",
        "Events": ["s3:ObjectCreated:*"]
      }
    ]
  }'

awslocal s3api put-bucket-notification-configuration \
  --bucket reviews-profanity \
  --notification-configuration '{
    "LambdaFunctionConfigurations": [
      {
        "Id": "sent",
        "LambdaFunctionArn": "arn:aws:lambda:us-east-1:000000000000:function:sentiment-analysis",
        "Events": ["s3:ObjectCreated:*"]
      }
    ]
  }'

echo "LocalStack ready"
