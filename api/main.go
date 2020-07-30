package main

import (
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
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/forecastservice"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
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

const layout              string = "2006-01-02 15:04"
const layout2             string = "20060102150405.000"
const layout3             string = "2006-01-02 00:00:00"
const idPrefix            string = "id"
const bucketName          string = "your-bucket"
const bucketRegion        string = "ap-northeast-1"
const bucketPath          string = "csv"
const bucketResultPath    string = "result"
const forecastRegion      string = "ap-northeast-1"
const forecastRoleArn     string = "arn:aws:iam::0:role/your-role"

func HandleRequest(ctx context.Context, request events.APIGatewayProxyRequest) (Response, error) {
	var jsonBytes []byte
	var err error
	d := make(map[string]string)
	json.Unmarshal([]byte(request.Body), &d)
	if v, ok := d["action"]; ok {
		switch v {
		case "senddata" :
			if data, ok := d["data"]; ok {
				res, e := sendData(data)
				if e != nil {
					err = e
				} else {
					jsonBytes, _ = json.Marshal(APIResponse{Message: res})
				}
			}
		case "checkimport" :
			if id, ok := d["id"]; ok {
				res, e := checkImport(id)
				if e != nil {
					err = e
				} else {
					jsonBytes, _ = json.Marshal(APIResponse{Message: res})
				}
			}
		case "checkpredictor" :
			if id, ok := d["id"]; ok {
				res, e := checkPredictor(id)
				if e != nil {
					err = e
				} else {
					jsonBytes, _ = json.Marshal(APIResponse{Message: res})
				}
			}
		case "checkforecast" :
			if id, ok := d["id"]; ok {
				res, e := checkForecast(id)
				if e != nil {
					err = e
				} else {
					jsonBytes, _ = json.Marshal(APIResponse{Message: res})
				}
			}
		case "checkexport" :
			if id, ok := d["id"]; ok {
				res, e := checkExport(id)
				if e != nil {
					err = e
				} else {
					jsonBytes, _ = json.Marshal(APIResponse{Message: res})
				}
			}
		case "getresult" :
			if id, ok := d["id"]; ok {
				res, e := getResult(id)
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

func getForecastservice() *forecastservice.ForecastService {
	return forecastservice.New(session.New(), &aws.Config{
		Region: aws.String(forecastRegion),
	})
}

func getS3() *s3.S3 {
	return s3.New(session.New(), &aws.Config{
		Region: aws.String(bucketRegion),
	})
}

func getSession()(*session.Session, error) {
	return session.NewSession(&aws.Config{
		Region: aws.String(bucketRegion)},
	)
}

func getForecastId(id string) string {
	return idPrefix + id
}

func createDatasetGroup(id string)(string, error) {
	svc := getForecastservice()

	input := &forecastservice.CreateDatasetGroupInput{
		DatasetGroupName: aws.String(getForecastId(id)),
		Domain: aws.String("CUSTOM"),
	}
	res, err := svc.CreateDatasetGroup(input)
	if err != nil {
		return "", err
	}
	return aws.StringValue(res.DatasetGroupArn), nil
}

func createDataset(id string)(string, error) {
	svc := getForecastservice()

	input := &forecastservice.CreateDatasetInput{
		DatasetName: aws.String(getForecastId(id)),
		DataFrequency: aws.String("D"),
		DatasetType: aws.String("TARGET_TIME_SERIES"),
		Domain: aws.String("CUSTOM"),
		Schema: &forecastservice.Schema{
			Attributes: []*forecastservice.SchemaAttribute{
				{
					AttributeName: aws.String("item_id"),
					AttributeType: aws.String("string"),
				},
				{
					AttributeName: aws.String("timestamp"),
					AttributeType: aws.String("timestamp"),
				},
				{
					AttributeName: aws.String("target_value"),
					AttributeType: aws.String("float"),
				},
			},
		},
	}
	res, err := svc.CreateDataset(input)
	if err != nil {
		return "", err
	}
	return aws.StringValue(res.DatasetArn), nil
}

func createDatasetImportJob(id string, datasetArn string, path string, roleArn string)(string, error) {
	svc := getForecastservice()

	input := &forecastservice.CreateDatasetImportJobInput{
		DatasetImportJobName: aws.String(getForecastId(id)),
		DatasetArn: aws.String(datasetArn),
		DataSource: &forecastservice.DataSource{
			S3Config: &forecastservice.S3Config{
				Path: aws.String(path),
				RoleArn: aws.String(roleArn),
			},
		},
	}
	res, err := svc.CreateDatasetImportJob(input)
	if err != nil {
		return "", err
	}
	return aws.StringValue(res.DatasetImportJobArn), nil
}

func createForecast(id string, predictorArn string)(string, error) {
	svc := getForecastservice()

	input := &forecastservice.CreateForecastInput{
		ForecastName: aws.String(getForecastId(id)),
		PredictorArn: aws.String(predictorArn),
	}
	res, err := svc.CreateForecast(input)
	if err != nil {
		return "", err
	}
	return aws.StringValue(res.ForecastArn), nil
}

func createForecastExportJob(id string, forecastArn string, path string, roleArn string)(string, error) {
	svc := getForecastservice()

	input := &forecastservice.CreateForecastExportJobInput{
		ForecastExportJobName: aws.String(getForecastId(id)),
		ForecastArn: aws.String(forecastArn),
		Destination: &forecastservice.DataDestination{
			S3Config: &forecastservice.S3Config{
				Path: aws.String(path),
				RoleArn: aws.String(roleArn),
			},
		},
	}
	res, err := svc.CreateForecastExportJob(input)
	if err != nil {
		return "", err
	}
	return aws.StringValue(res.ForecastExportJobArn), nil
}

func createPredictor(id string, datasetGroupArn string)(string, error) {
	svc := getForecastservice()

	input := &forecastservice.CreatePredictorInput{
		PredictorName: aws.String(getForecastId(id)),
		PerformAutoML: aws.Bool(true),
		ForecastHorizon: aws.Int64(10),
		InputDataConfig: &forecastservice.InputDataConfig{
			DatasetGroupArn: aws.String(datasetGroupArn),
		},
		FeaturizationConfig: &forecastservice.FeaturizationConfig{
			ForecastFrequency: aws.String("D"),
		},
	}
	res, err := svc.CreatePredictor(input)
	if err != nil {
		return "", err
	}
	return aws.StringValue(res.PredictorArn), nil
}

func getDatasetGroup(id string) *forecastservice.DatasetGroupSummary {
	svc := getForecastservice()

	input := &forecastservice.ListDatasetGroupsInput{}
	res, err := svc.ListDatasetGroups(input)
	if err != nil {
		return nil
	}
	for _, v := range res.DatasetGroups {
		if getForecastId(id) == aws.StringValue(v.DatasetGroupName) {
			return v
		}
	}
	return nil
}

func getDataset(id string) *forecastservice.DatasetSummary {
	svc := getForecastservice()

	input := &forecastservice.ListDatasetsInput{}
	res, err := svc.ListDatasets(input)
	if err != nil {
		return nil
	}
	for _, v := range res.Datasets {
		if getForecastId(id) == aws.StringValue(v.DatasetName) {
			return v
		}
	}
	return nil
}

func getDatasetImportJob(id string) *forecastservice.DatasetImportJobSummary {
	svc := getForecastservice()

	input := &forecastservice.ListDatasetImportJobsInput{}
	res, err := svc.ListDatasetImportJobs(input)
	if err != nil {
		return nil
	}
	for _, v := range res.DatasetImportJobs {
		if getForecastId(id) == aws.StringValue(v.DatasetImportJobName) {
			return v
		}
	}
	return nil
}

func getForecast(id string) *forecastservice.ForecastSummary {
	svc := getForecastservice()

	input := &forecastservice.ListForecastsInput{}
	res, err := svc.ListForecasts(input)
	if err != nil {
		return nil
	}
	for _, v := range res.Forecasts {
		if getForecastId(id) == aws.StringValue(v.ForecastName) {
			return v
		}
	}
	return nil
}

func getForecastExportJob(id string) *forecastservice.ForecastExportJobSummary {
	svc := getForecastservice()

	input := &forecastservice.ListForecastExportJobsInput{}
	res, err := svc.ListForecastExportJobs(input)
	if err != nil {
		return nil
	}
	for _, v := range res.ForecastExportJobs {
		if getForecastId(id) == aws.StringValue(v.ForecastExportJobName) {
			return v
		}
	}
	return nil
}

func getPredictor(id string) *forecastservice.PredictorSummary {
	svc := getForecastservice()

	input := &forecastservice.ListPredictorsInput{}
	res, err := svc.ListPredictors(input)
	if err != nil {
		return nil
	}
	for _, v := range res.Predictors {
		if getForecastId(id) == aws.StringValue(v.PredictorName) {
			return v
		}
	}
	return nil
}

func updateDatasetGroup(datasetArn string, datasetGroupArn string) error {
	svc := getForecastservice()

	input := &forecastservice.UpdateDatasetGroupInput{
		DatasetArns: []*string{aws.String(datasetArn)},
		DatasetGroupArn: aws.String(datasetGroupArn),
	}
	_, err := svc.UpdateDatasetGroup(input)
	if err != nil {
		return err
	}
	return nil
}

func getObjectKey(id string) string {
	svc := getS3()

	input := &s3.ListObjectsInput{
		Bucket: aws.String(bucketName),
	}
	res, err := svc.ListObjects(input)
	if err != nil {
		return ""
	}
	for _, v := range res.Contents {
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
	sess, err := getSession()
	if err != nil {
		log.Print(err)
		return err
	}
	for i, v := range values {
		t_ := t.AddDate(0, 0, i - len(values))
		stringData += "v," + t_.Format(layout3) + "," + strconv.FormatFloat(v, 'f', -1, 64) + "\n"
	}
	uploader := s3manager.NewUploader(sess)
	_, err = uploader.Upload(&s3manager.UploadInput{
		ACL: aws.String("private"),
		Bucket: aws.String(bucketName),
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

func sendData(data string)(string, error) {
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
	datasetGroupArn, err := createDatasetGroup(progressId)
	if err != nil {
		log.Print(err)
		return "", err
	}

	// CreateDataset
	datasetArn, err := createDataset(progressId)
	if err != nil {
		log.Print(err)
		return "", err
	}

	// UpdateDatasetGroup
	err = updateDatasetGroup(datasetArn, datasetGroupArn)
	if err != nil {
		log.Print(err)
		return "", err
	}
	return progressId, nil
}

func checkImport(id string)(string, error) {
	// GetDatasetImportJob
	res := getDatasetImportJob(id)
	if res == nil {
		// CreateDatasetImportJob
		ds := getDataset(id)
		if ds == nil {
			return "", fmt.Errorf("Error: %s", "No Dataset.")
		}
		path := "s3://" + bucketName + "/" + bucketPath + "/" + getForecastId(id) + ".csv"
		_, err := createDatasetImportJob(id, aws.StringValue(ds.DatasetArn), path, forecastRoleArn)
		if err != nil {
			log.Print(err)
			return "", err
		}
		return "Start", nil
	}
	return aws.StringValue(res.Status), nil
}

func checkPredictor(id string)(string, error) {
	// GetPredictor
	res := getPredictor(id)
	if res == nil {
		// CreatePredictor
		dsg := getDatasetGroup(id)
		if dsg == nil {
			return "", fmt.Errorf("Error: %s", "No DatasetGroup.")
		}
		_, err := createPredictor(id, aws.StringValue(dsg.DatasetGroupArn))
		if err != nil {
			log.Print(err)
			return "", err
		}
		return "Start", nil
	}
	return aws.StringValue(res.Status), nil
}

func checkForecast(id string)(string, error) {
	// GetForecast
	res := getForecast(id)
	if res == nil {
		// CreateForecast
		pre := getPredictor(id)
		if pre == nil {
			return "", fmt.Errorf("Error: %s", "No Predictor.")
		}
		_, err := createForecast(id, aws.StringValue(pre.PredictorArn))
		if err != nil {
			log.Print(err)
			return "", err
		}
		return "Start", nil
	}
	return aws.StringValue(res.Status), nil
}

func checkExport(id string)(string, error) {
	// GetForecastExportJob
	res := getForecastExportJob(id)
	if res == nil {
		// CreateForecastExportJob
		fct := getForecast(id)
		if fct == nil {
			return "", fmt.Errorf("Error: %s", "No Forecast.")
		}
		path := "s3://" + bucketName + "/" + bucketResultPath + "/" + getForecastId(id)
		_, err := createForecastExportJob(id, aws.StringValue(fct.ForecastArn), path, forecastRoleArn)
		if err != nil {
			log.Print(err)
			return "", err
		}
		return "Start", nil
	}
	return aws.StringValue(res.Status), nil
}

func getResult(id string)(string, error) {
	resultData := ""
	objectKey := getObjectKey(id)
	if len(objectKey) == 0 {
		return "", fmt.Errorf("Error: %s", "No ObjectKey.")
	}
	svc := getS3()
	obj, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		return "", err
	}

	rc := obj.Body
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

func main() {
	lambda.Start(HandleRequest)
}
