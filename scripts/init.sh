#!/bin/bash
ROLE_NAME='forecast-main-role'
aws iam create-role --role-name $ROLE_NAME --path /service-role/ --assume-role-policy-document file://`pwd`/`dirname $0`/policy.json
ROLE_ARN=`aws iam get-role --role-name $ROLE_NAME | jq -r  .'Role.Arn'`
aws iam attach-role-policy --role-name $ROLE_NAME --policy-arn "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
echo 'Creating template...'
`dirname $0`/create_template.sh

echo 'Creating function.zip...'
`dirname $0`/create_function.sh

echo 'Create Lambda-Function...'
cd `dirname $0`/../
aws lambda create-function \
	--function-name your_function_name \
	--runtime provided.al2 \
	--role $ROLE_ARN \
	--handler bootstrap \
	--zip-file fileb://`pwd`/function.zip \
	--region ap-northeast-1
