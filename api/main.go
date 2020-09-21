package main

import (
	"os"
	"fmt"
	"log"
	"time"
	"bytes"
	"strconv"
	"strings"
	"context"
	"io/ioutil"
	"encoding/json"
	"github.com/jszwec/csvutil"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/forecast"
	"github.com/aws/aws-sdk-go-v2/service/s3/s3manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type APIResponse struct {
	Message  string `json:"message"`
}

type ResultData struct {
	ID   string  `csv:"item_id"`
	Data string  `csv:"date"`
	P10  float64 `csv:"p10"`
	P50  float64 `csv:"p50"`
	P90  float64 `csv:"p90"`
}

type Response events.APIGatewayProxyResponse

var cfg aws.Config
var s3Client *s3.Client
var forecastClient *forecast.Client

const layout              string = "2006-01-02 15:04"
const layout2             string = "20060102150405.000"
const layout3             string = "2006-01-02 00:00:00"
const idPrefix            string = "id"
const bucketPath          string = "csv"
const bucketResultPath    string = "result"

func HandleRequest(ctx context.Context, request events.APIGatewayProxyRequest) (Response, error) {
	var jsonBytes []byte
	var err error
	d := make(map[string]string)
	json.Unmarshal([]byte(request.Body), &d)
	if v, ok := d["action"]; ok {
		switch v {
		case "senddata" :
			if data, ok := d["data"]; ok {
				res, e := sendData(ctx, data)
				if e != nil {
					err = e
				} else {
					jsonBytes, _ = json.Marshal(APIResponse{Message: res})
				}
			}
		case "checkimport" :
			if id, ok := d["id"]; ok {
				res, e := checkImport(ctx, id)
				if e != nil {
					err = e
				} else {
					jsonBytes, _ = json.Marshal(APIResponse{Message: res})
				}
			}
		case "checkpredictor" :
			if id, ok := d["id"]; ok {
				res, e := checkPredictor(ctx, id)
				if e != nil {
					err = e
				} else {
					jsonBytes, _ = json.Marshal(APIResponse{Message: res})
				}
			}
		case "checkforecast" :
			if id, ok := d["id"]; ok {
				res, e := checkForecast(ctx, id)
				if e != nil {
					err = e
				} else {
					jsonBytes, _ = json.Marshal(APIResponse{Message: res})
				}
			}
		case "checkexport" :
			if id, ok := d["id"]; ok {
				res, e := checkExport(ctx, id)
				if e != nil {
					err = e
				} else {
					jsonBytes, _ = json.Marshal(APIResponse{Message: res})
				}
			}
		case "getresult" :
			if id, ok := d["id"]; ok {
				res, e := getResult(ctx, id)
				if e != nil {
					err = e
				} else {
					jsonBytes, _ = json.Marshal(APIResponse{Message: res})
				}
			}
		}
	}
	log.Print(request.RequestContext.Identity.SourceIP)
	if err != nil {
		log.Print(err)
		jsonBytes, _ = json.Marshal(APIResponse{Message: fmt.Sprint(err)})
		return Response{
			StatusCode: 500,
			Body: string(jsonBytes),
		}, nil
	}
	return Response {
		StatusCode: 200,
		Body: string(jsonBytes),
	}, nil
}

func getForecastId(id string) string {
	return idPrefix + id
}

func createDatasetGroup(ctx context.Context, id string)(string, error) {
	if forecastClient == nil {
		forecastClient = forecast.New(cfg)
	}

	input := &forecast.CreateDatasetGroupInput{
		DatasetGroupName: aws.String(getForecastId(id)),
		Domain: forecast.DomainCustom,
	}
	req := forecastClient.CreateDatasetGroupRequest(input)
	res, err := req.Send(ctx)
	if err != nil {
		return "", err
	}
	return aws.StringValue(res.CreateDatasetGroupOutput.DatasetGroupArn), nil
}

func createDataset(ctx context.Context, id string)(string, error) {
	if forecastClient == nil {
		forecastClient = forecast.New(cfg)
	}

	input := &forecast.CreateDatasetInput{
		DatasetName: aws.String(getForecastId(id)),
		DataFrequency: aws.String("D"),
		DatasetType: forecast.DatasetTypeTargetTimeSeries,
		Domain: forecast.DomainCustom,
		Schema: &forecast.Schema{
			Attributes: []forecast.SchemaAttribute{
				{
					AttributeName: aws.String("item_id"),
					AttributeType: forecast.AttributeTypeString,
				},
				{
					AttributeName: aws.String("timestamp"),
					AttributeType: forecast.AttributeTypeTimestamp,
				},
				{
					AttributeName: aws.String("target_value"),
					AttributeType: forecast.AttributeTypeFloat,
				},
			},
		},
	}
	req := forecastClient.CreateDatasetRequest(input)
	res, err := req.Send(ctx)
	if err != nil {
		return "", err
	}
	return aws.StringValue(res.CreateDatasetOutput.DatasetArn), nil
}

func createDatasetImportJob(ctx context.Context, id string, datasetArn string, path string, roleArn string)(string, error) {
	if forecastClient == nil {
		forecastClient = forecast.New(cfg)
	}

	input := &forecast.CreateDatasetImportJobInput{
		DatasetImportJobName: aws.String(getForecastId(id)),
		DatasetArn: aws.String(datasetArn),
		DataSource: &forecast.DataSource{
			S3Config: &forecast.S3Config{
				Path: aws.String(path),
				RoleArn: aws.String(roleArn),
			},
		},
	}
	req := forecastClient.CreateDatasetImportJobRequest(input)
	res, err := req.Send(ctx)
	if err != nil {
		return "", err
	}
	return aws.StringValue(res.CreateDatasetImportJobOutput.DatasetImportJobArn), nil
}

func createForecast(ctx context.Context, id string, predictorArn string)(string, error) {
	if forecastClient == nil {
		forecastClient = forecast.New(cfg)
	}

	input := &forecast.CreateForecastInput{
		ForecastName: aws.String(getForecastId(id)),
		PredictorArn: aws.String(predictorArn),
	}
	req := forecastClient.CreateForecastRequest(input)
	res, err := req.Send(ctx)
	if err != nil {
		return "", err
	}
	return aws.StringValue(res.CreateForecastOutput.ForecastArn), nil
}

func createForecastExportJob(ctx context.Context, id string, forecastArn string, path string, roleArn string)(string, error) {
	if forecastClient == nil {
		forecastClient = forecast.New(cfg)
	}

	input := &forecast.CreateForecastExportJobInput{
		ForecastExportJobName: aws.String(getForecastId(id)),
		ForecastArn: aws.String(forecastArn),
		Destination: &forecast.DataDestination{
			S3Config: &forecast.S3Config{
				Path: aws.String(path),
				RoleArn: aws.String(roleArn),
			},
		},
	}
	req := forecastClient.CreateForecastExportJobRequest(input)
	res, err := req.Send(ctx)
	if err != nil {
		return "", err
	}
	return aws.StringValue(res.CreateForecastExportJobOutput.ForecastExportJobArn), nil
}

func createPredictor(ctx context.Context, id string, datasetGroupArn string)(string, error) {
	if forecastClient == nil {
		forecastClient = forecast.New(cfg)
	}

	input := &forecast.CreatePredictorInput{
		PredictorName: aws.String(getForecastId(id)),
		PerformAutoML: aws.Bool(true),
		ForecastHorizon: aws.Int64(10),
		InputDataConfig: &forecast.InputDataConfig{
			DatasetGroupArn: aws.String(datasetGroupArn),
		},
		FeaturizationConfig: &forecast.FeaturizationConfig{
			ForecastFrequency: aws.String("D"),
		},
	}
	req := forecastClient.CreatePredictorRequest(input)
	res, err := req.Send(ctx)
	if err != nil {
		return "", err
	}
	return aws.StringValue(res.CreatePredictorOutput.PredictorArn), nil
}

func getDatasetGroup(ctx context.Context, id string) forecast.DatasetGroupSummary {
	if forecastClient == nil {
		forecastClient = forecast.New(cfg)
	}

	input := &forecast.ListDatasetGroupsInput{}
	req := forecastClient.ListDatasetGroupsRequest(input)
	res, err := req.Send(ctx)
	if err != nil {
		return forecast.DatasetGroupSummary{}
	}
	for _, v := range res.ListDatasetGroupsOutput.DatasetGroups {
		if getForecastId(id) == aws.StringValue(v.DatasetGroupName) {
			return v
		}
	}
	return forecast.DatasetGroupSummary{}
}

func getDataset(ctx context.Context, id string) forecast.DatasetSummary {
	if forecastClient == nil {
		forecastClient = forecast.New(cfg)
	}

	input := &forecast.ListDatasetsInput{}
	req := forecastClient.ListDatasetsRequest(input)
	res, err := req.Send(ctx)
	if err != nil {
		return forecast.DatasetSummary{}
	}
	for _, v := range res.ListDatasetsOutput.Datasets {
		if getForecastId(id) == aws.StringValue(v.DatasetName) {
			return v
		}
	}
	return forecast.DatasetSummary{}
}

func getDatasetImportJob(ctx context.Context, id string) forecast.DatasetImportJobSummary {
	if forecastClient == nil {
		forecastClient = forecast.New(cfg)
	}

	input := &forecast.ListDatasetImportJobsInput{}
	req := forecastClient.ListDatasetImportJobsRequest(input)
	res, err := req.Send(ctx)
	if err != nil {
		return forecast.DatasetImportJobSummary{}
	}
	for _, v := range res.ListDatasetImportJobsOutput.DatasetImportJobs {
		if getForecastId(id) == aws.StringValue(v.DatasetImportJobName) {
			return v
		}
	}
	return forecast.DatasetImportJobSummary{}
}

func getForecast(ctx context.Context, id string) forecast.ForecastSummary {
	if forecastClient == nil {
		forecastClient = forecast.New(cfg)
	}

	input := &forecast.ListForecastsInput{}
	req := forecastClient.ListForecastsRequest(input)
	res, err := req.Send(ctx)
	if err != nil {
		return forecast.ForecastSummary{}
	}
	for _, v := range res.ListForecastsOutput.Forecasts {
		if getForecastId(id) == aws.StringValue(v.ForecastName) {
			return v
		}
	}
	return forecast.ForecastSummary{}
}

func getForecastExportJob(ctx context.Context, id string) forecast.ForecastExportJobSummary {
	if forecastClient == nil {
		forecastClient = forecast.New(cfg)
	}

	input := &forecast.ListForecastExportJobsInput{}
	req := forecastClient.ListForecastExportJobsRequest(input)
	res, err := req.Send(ctx)
	if err != nil {
		return forecast.ForecastExportJobSummary{}
	}
	for _, v := range res.ListForecastExportJobsOutput.ForecastExportJobs {
		if getForecastId(id) == aws.StringValue(v.ForecastExportJobName) {
			return v
		}
	}
	return forecast.ForecastExportJobSummary{}
}

func getPredictor(ctx context.Context, id string) forecast.PredictorSummary {
	if forecastClient == nil {
		forecastClient = forecast.New(cfg)
	}

	input := &forecast.ListPredictorsInput{}
	req := forecastClient.ListPredictorsRequest(input)
	res, err := req.Send(ctx)
	if err != nil {
		return forecast.PredictorSummary{}
	}
	for _, v := range res.ListPredictorsOutput.Predictors {
		if getForecastId(id) == aws.StringValue(v.PredictorName) {
			return v
		}
	}
	return forecast.PredictorSummary{}
}

func updateDatasetGroup(ctx context.Context, datasetArn string, datasetGroupArn string) error {
	if forecastClient == nil {
		forecastClient = forecast.New(cfg)
	}

	input := &forecast.UpdateDatasetGroupInput{
		DatasetArns: []string{datasetArn},
		DatasetGroupArn: aws.String(datasetGroupArn),
	}
	req := forecastClient.UpdateDatasetGroupRequest(input)
	_, err := req.Send(ctx)
	if err != nil {
		return err
	}
	return nil
}

func getObjectKey(ctx context.Context, id string) string {
	if s3Client == nil {
		s3Client = s3.New(cfg)
	}

	input := &s3.ListObjectsInput{
		Bucket: aws.String(os.Getenv("BUCKET_NAME")),
	}
	req := s3Client.ListObjectsRequest(input)
	res, err := req.Send(ctx)
	if err != nil {
		return ""
	}
	for _, v := range res.ListObjectsOutput.Contents {
		if strings.HasPrefix(aws.StringValue(v.Key), bucketResultPath + "/" + getForecastId(id)) && strings.HasSuffix(aws.StringValue(v.Key), "part0.csv") {
			return aws.StringValue(v.Key)
		}
	}
	return ""
}

func uploadData(id string, values []float64) error {
	t := time.Now()
	stringData := "item_id,timestamp,target_value\n"
	contentType := "text/csv"
	filename := getForecastId(id) + ".csv"
	for i, v := range values {
		t_ := t.AddDate(0, 0, i - len(values))
		stringData += "v," + t_.Format(layout3) + "," + strconv.FormatFloat(v, 'f', -1, 64) + "\n"
	}
	uploader := s3manager.NewUploader(cfg)
	_, err := uploader.Upload(&s3manager.UploadInput{
		ACL: s3.ObjectCannedACLPrivate,
		Bucket: aws.String(os.Getenv("BUCKET_NAME")),
		Key: aws.String(bucketPath + "/" + filename),
		Body: bytes.NewReader([]byte(stringData)),
		ContentType: aws.String(contentType),
	})
	if err != nil {
		log.Print(err)
		return err
	}
	return nil
}

func sendData(ctx context.Context, data string)(string, error) {
	mx := 100
	mn := 30
	var values []float64
	if err := json.Unmarshal([]byte(data), &values); err != nil {
		log.Print(err)
		return "", err
	}
	if len(values) < mn || len(values) > mx {
		return "", fmt.Errorf("Error: %s", "Invalid Data Size.")
	}
	t := time.Now()
	progressId := t.Format(layout2)[:14] + t.Format(layout2)[15:]

	// Upload Data
	err := uploadData(progressId, values)
	if err != nil {
		log.Print(err)
		return "", err
	}

	// CreateDatasetGroup
	datasetGroupArn, err := createDatasetGroup(ctx, progressId)
	if err != nil {
		log.Print(err)
		return "", err
	}

	// CreateDataset
	datasetArn, err := createDataset(ctx, progressId)
	if err != nil {
		log.Print(err)
		return "", err
	}

	// UpdateDatasetGroup
	err = updateDatasetGroup(ctx, datasetArn, datasetGroupArn)
	if err != nil {
		log.Print(err)
		return "", err
	}
	return progressId, nil
}

func checkImport(ctx context.Context, id string)(string, error) {
	// GetDatasetImportJob
	res := getDatasetImportJob(ctx, id)
	if len(aws.StringValue(res.Status)) < 1 {
		// CreateDatasetImportJob
		ds := getDataset(ctx, id)
		if len(aws.StringValue(ds.DatasetArn)) < 1 {
			return "", fmt.Errorf("Error: %s", "No Dataset.")
		}
		path := "s3://" + os.Getenv("BUCKET_NAME") + "/" + bucketPath + "/" + getForecastId(id) + ".csv"
		_, err := createDatasetImportJob(ctx, id, aws.StringValue(ds.DatasetArn), path, os.Getenv("FORECAST_ROLE_ARN"))
		if err != nil {
			log.Print(err)
			return "", err
		}
		return "Start", nil
	}
	return aws.StringValue(res.Status), nil
}

func checkPredictor(ctx context.Context, id string)(string, error) {
	// GetPredictor
	res := getPredictor(ctx, id)
	if len(aws.StringValue(res.Status)) < 1 {
		// CreatePredictor
		dsg := getDatasetGroup(ctx, id)
		if len(aws.StringValue(dsg.DatasetGroupArn)) < 1 {
			return "", fmt.Errorf("Error: %s", "No DatasetGroup.")
		}
		_, err := createPredictor(ctx, id, aws.StringValue(dsg.DatasetGroupArn))
		if err != nil {
			log.Print(err)
			return "", err
		}
		return "Start", nil
	}
	return aws.StringValue(res.Status), nil
}

func checkForecast(ctx context.Context, id string)(string, error) {
	// GetForecast
	res := getForecast(ctx, id)
	if len(aws.StringValue(res.Status)) < 1 {
		// CreateForecast
		pre := getPredictor(ctx, id)
		if len(aws.StringValue(pre.PredictorArn)) < 1 {
			return "", fmt.Errorf("Error: %s", "No Predictor.")
		}
		_, err := createForecast(ctx, id, aws.StringValue(pre.PredictorArn))
		if err != nil {
			log.Print(err)
			return "", err
		}
		return "Start", nil
	}
	return aws.StringValue(res.Status), nil
}

func checkExport(ctx context.Context, id string)(string, error) {
	// GetForecastExportJob
	res := getForecastExportJob(ctx, id)
	if len(aws.StringValue(res.Status)) < 1 {
		// CreateForecastExportJob
		fct := getForecast(ctx, id)
		if len(aws.StringValue(fct.ForecastArn)) < 1 {
			return "", fmt.Errorf("Error: %s", "No Forecast.")
		}
		path := "s3://" + os.Getenv("BUCKET_NAME") + "/" + bucketResultPath + "/" + getForecastId(id)
		_, err := createForecastExportJob(ctx, id, aws.StringValue(fct.ForecastArn), path, os.Getenv("FORECAST_ROLE_ARN"))
		if err != nil {
			log.Print(err)
			return "", err
		}
		return "Start", nil
	}
	return aws.StringValue(res.Status), nil
}

func getResult(ctx context.Context, id string)(string, error) {
	resultData := ""
	objectKey := getObjectKey(ctx, id)
	if len(objectKey) == 0 {
		return "", fmt.Errorf("Error: %s", "No ObjectKey.")
	}
	if s3Client == nil {
		s3Client = s3.New(cfg)
	}
	req := s3Client.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(os.Getenv("BUCKET_NAME")),
		Key:    aws.String(objectKey),
	})
	res, err := req.Send(ctx)
	if err != nil {
		return "", err
	}

	rc := res.GetObjectOutput.Body
	defer rc.Close()
	tmpData, err := ioutil.ReadAll(rc)
	if err != nil {
		log.Println(err)
		return "", err
	}
	var values []ResultData
	if err := csvutil.Unmarshal(tmpData, &values); err != nil {
		log.Println(err)
		return "", err
	} else {
		for _, v := range values {
			resultData += strconv.FormatFloat(v.P50, 'f', -1, 64) + ","
		}
	}
	return "[" + resultData[:len(resultData)-1] + "]", nil
}

func init() {
	var err error
	cfg, err = external.LoadDefaultAWSConfig()
	cfg.Region = os.Getenv("REGION")
	if err != nil {
		log.Print(err)
	}
}

func main() {
	lambda.Start(HandleRequest)
}
