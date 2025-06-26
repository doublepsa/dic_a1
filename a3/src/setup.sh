#!/bin/bash

# Create S3 buckets
echo "Creating S3 buckets..."
awslocal s3 mb s3://reviews-input
awslocal s3 mb s3://reviews-processed

# Create SSM parameters
echo "Setting up SSM parameters..."
awslocal ssm put-parameter --name "/review-app/buckets/input" --value "reviews-input" --type "String"
awslocal ssm put-parameter --name "/review-app/buckets/processed" --value "reviews-processed" --type "String"

# Create deployment package for preprocess lambda
echo "Creating lambda deployment package..."
cd lambdas/preprocess
zip -r preprocess.zip handler.py
cd ../..

# Create the lambda function
echo "Creating preprocess lambda function..."
awslocal lambda create-function \
    --function-name preprocess \
    --runtime python3.11 \
    --role arn:aws:iam::000000000000:role/lambda-role \
    --handler handler.handler \
    --zip-file fileb://lambdas/preprocess/preprocess.zip \
    --environment Variables='{STAGE=local}'

# Set up S3 trigger for the lambda
echo "Setting up S3 trigger..."
awslocal s3api put-bucket-notification-configuration \
    --bucket reviews-input \
    --notification-configuration '{
        "LambdaFunctionConfigurations": [
            {
                "Id": "preprocess-trigger",
                "LambdaFunctionArn": "arn:aws:lambda:us-east-1:000000000000:function:preprocess",
                "Events": ["s3:ObjectCreated:*"]
            }
        ]
    }'

echo "Setup complete! You can now upload review files to s3://reviews-input"
