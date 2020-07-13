#!/bin/bash
ROLE_NAME='forecast-api-role'
aws iam create-role --role-name $ROLE_NAME --path /service-role/ --assume-role-policy-document file://`pwd`/`dirname $0`/policy.json
ROLE_ARN=`aws iam get-role --role-name $ROLE_NAME | jq -r  .'Role.Arn'`
aws iam attach-role-policy --role-name $ROLE_NAME --policy-arn "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
aws iam attach-role-policy --role-name $ROLE_NAME --policy-arn "arn:aws:iam::aws:policy/AmazonS3FullAccess"
aws iam attach-role-policy --role-name $ROLE_NAME --policy-arn "arn:aws:iam::aws:policy/AmazonForecastFullAccess"
echo 'Create Lambda-Function...'
cd `dirname $0`/../
rm function.zip
rm main
GOOS=linux go build main.go
zip -g function.zip main
aws lambda create-function \
	--function-name your_api_function_name \
	--runtime go1.x \
	--role $ROLE_ARN \
	--handler main \
	--zip-file fileb://`pwd`/function.zip \
	--region ap-northeast-1
