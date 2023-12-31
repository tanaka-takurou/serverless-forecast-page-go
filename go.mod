module github.com/tanaka-takurou/serverless-forecast-page-go

go 1.21

require (
	github.com/aws/aws-lambda-go latest
	github.com/aws/aws-sdk-go-v2 latest
	github.com/aws/aws-sdk-go-v2/config latest
	github.com/aws/aws-sdk-go-v2/service/forecast latest
	github.com/aws/aws-sdk-go-v2/service/s3 latest
	github.com/jszwec/csvutil latest
)
