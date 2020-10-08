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
	"reflect"
	"io/ioutil"
	"encoding/json"
	"github.com/jszwec/csvutil"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/forecast"
	ftypes "github.com/aws/aws-sdk-go-v2/service/forecast/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	stypes "github.com/aws/aws-sdk-go-v2/service/s3/types"
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
		forecastClient = getForecastClient()
	}

	input := &forecast.CreateDatasetGroupInput{
		DatasetGroupName: aws.String(getForecastId(id)),
		Domain: ftypes.DomainCustom,
	}
	res, err := forecastClient.CreateDatasetGroup(ctx, input)
	if err != nil {
		return "", err
	}
	return stringValue(res.DatasetGroupArn), nil
}

func createDataset(ctx context.Context, id string)(string, error) {
	if forecastClient == nil {
		forecastClient = getForecastClient()
	}

	input := &forecast.CreateDatasetInput{
		DatasetName: aws.String(getForecastId(id)),
		DataFrequency: aws.String("D"),
		DatasetType: ftypes.DatasetTypeTarget_time_series,
		Domain: ftypes.DomainCustom,
		Schema: &ftypes.Schema{
			Attributes: []*ftypes.SchemaAttribute{
				{
					AttributeName: aws.String("item_id"),
					AttributeType: ftypes.AttributeTypeString,
				},
				{
					AttributeName: aws.String("timestamp"),
					AttributeType: ftypes.AttributeTypeTimestamp,
				},
				{
					AttributeName: aws.String("target_value"),
					AttributeType: ftypes.AttributeTypeFloat,
				},
			},
		},
	}
	res, err := forecastClient.CreateDataset(ctx, input)
	if err != nil {
		return "", err
	}
	return stringValue(res.DatasetArn), nil
}

func createDatasetImportJob(ctx context.Context, id string, datasetArn string, path string, roleArn string)(string, error) {
	if forecastClient == nil {
		forecastClient = getForecastClient()
	}

	input := &forecast.CreateDatasetImportJobInput{
		DatasetImportJobName: aws.String(getForecastId(id)),
		DatasetArn: aws.String(datasetArn),
		DataSource: &ftypes.DataSource{
			S3Config: &ftypes.S3Config{
				Path: aws.String(path),
				RoleArn: aws.String(roleArn),
			},
		},
	}
	res, err := forecastClient.CreateDatasetImportJob(ctx, input)
	if err != nil {
		return "", err
	}
	return stringValue(res.DatasetImportJobArn), nil
}

func createForecast(ctx context.Context, id string, predictorArn string)(string, error) {
	if forecastClient == nil {
		forecastClient = getForecastClient()
	}

	input := &forecast.CreateForecastInput{
		ForecastName: aws.String(getForecastId(id)),
		PredictorArn: aws.String(predictorArn),
	}
	res, err := forecastClient.CreateForecast(ctx, input)
	if err != nil {
		return "", err
	}
	return stringValue(res.ForecastArn), nil
}

func createForecastExportJob(ctx context.Context, id string, forecastArn string, path string, roleArn string)(string, error) {
	if forecastClient == nil {
		forecastClient = getForecastClient()
	}

	input := &forecast.CreateForecastExportJobInput{
		ForecastExportJobName: aws.String(getForecastId(id)),
		ForecastArn: aws.String(forecastArn),
		Destination: &ftypes.DataDestination{
			S3Config: &ftypes.S3Config{
				Path: aws.String(path),
				RoleArn: aws.String(roleArn),
			},
		},
	}
	res, err := forecastClient.CreateForecastExportJob(ctx, input)
	if err != nil {
		return "", err
	}
	return stringValue(res.ForecastExportJobArn), nil
}

func createPredictor(ctx context.Context, id string, datasetGroupArn string)(string, error) {
	if forecastClient == nil {
		forecastClient = getForecastClient()
	}

	input := &forecast.CreatePredictorInput{
		PredictorName: aws.String(getForecastId(id)),
		PerformAutoML: aws.Bool(true),
		ForecastHorizon: aws.Int32(10),
		InputDataConfig: &ftypes.InputDataConfig{
			DatasetGroupArn: aws.String(datasetGroupArn),
		},
		FeaturizationConfig: &ftypes.FeaturizationConfig{
			ForecastFrequency: aws.String("D"),
		},
	}
	res, err := forecastClient.CreatePredictor(ctx, input)
	if err != nil {
		return "", err
	}
	return stringValue(res.PredictorArn), nil
}

func getDatasetGroup(ctx context.Context, id string) ftypes.DatasetGroupSummary {
	if forecastClient == nil {
		forecastClient = getForecastClient()
	}

	input := &forecast.ListDatasetGroupsInput{}
	res, err := forecastClient.ListDatasetGroups(ctx, input)
	if err != nil {
		return ftypes.DatasetGroupSummary{}
	}
	for _, v := range res.DatasetGroups {
		if getForecastId(id) == stringValue(v.DatasetGroupName) {
			return *v
		}
	}
	return ftypes.DatasetGroupSummary{}
}

func getDataset(ctx context.Context, id string) ftypes.DatasetSummary {
	if forecastClient == nil {
		forecastClient = getForecastClient()
	}

	input := &forecast.ListDatasetsInput{}
	res, err := forecastClient.ListDatasets(ctx, input)
	if err != nil {
		return ftypes.DatasetSummary{}
	}
	for _, v := range res.Datasets {
		if getForecastId(id) == stringValue(v.DatasetName) {
			return *v
		}
	}
	return ftypes.DatasetSummary{}
}

func getDatasetImportJob(ctx context.Context, id string) ftypes.DatasetImportJobSummary {
	if forecastClient == nil {
		forecastClient = getForecastClient()
	}

	input := &forecast.ListDatasetImportJobsInput{}
	res, err := forecastClient.ListDatasetImportJobs(ctx, input)
	if err != nil {
		return ftypes.DatasetImportJobSummary{}
	}
	for _, v := range res.DatasetImportJobs {
		if getForecastId(id) == stringValue(v.DatasetImportJobName) {
			return *v
		}
	}
	return ftypes.DatasetImportJobSummary{}
}

func getForecast(ctx context.Context, id string) ftypes.ForecastSummary {
	if forecastClient == nil {
		forecastClient = getForecastClient()
	}

	input := &forecast.ListForecastsInput{}
	res, err := forecastClient.ListForecasts(ctx, input)
	if err != nil {
		return ftypes.ForecastSummary{}
	}
	for _, v := range res.Forecasts {
		if getForecastId(id) == stringValue(v.ForecastName) {
			return *v
		}
	}
	return ftypes.ForecastSummary{}
}

func getForecastExportJob(ctx context.Context, id string) ftypes.ForecastExportJobSummary {
	if forecastClient == nil {
		forecastClient = getForecastClient()
	}

	input := &forecast.ListForecastExportJobsInput{}
	res, err := forecastClient.ListForecastExportJobs(ctx, input)
	if err != nil {
		return ftypes.ForecastExportJobSummary{}
	}
	for _, v := range res.ForecastExportJobs {
		if getForecastId(id) == stringValue(v.ForecastExportJobName) {
			return *v
		}
	}
	return ftypes.ForecastExportJobSummary{}
}

func getPredictor(ctx context.Context, id string) ftypes.PredictorSummary {
	if forecastClient == nil {
		forecastClient = getForecastClient()
	}

	input := &forecast.ListPredictorsInput{}
	res, err := forecastClient.ListPredictors(ctx, input)
	if err != nil {
		return ftypes.PredictorSummary{}
	}
	for _, v := range res.Predictors {
		if getForecastId(id) == stringValue(v.PredictorName) {
			return *v
		}
	}
	return ftypes.PredictorSummary{}
}

func updateDatasetGroup(ctx context.Context, datasetArn string, datasetGroupArn string) error {
	if forecastClient == nil {
		forecastClient = getForecastClient()
	}

	input := &forecast.UpdateDatasetGroupInput{
		DatasetArns: []*string{aws.String(datasetArn)},
		DatasetGroupArn: aws.String(datasetGroupArn),
	}
	_, err := forecastClient.UpdateDatasetGroup(ctx, input)
	if err != nil {
		return err
	}
	return nil
}

func getObjectKey(ctx context.Context, id string) string {
	if s3Client == nil {
		s3Client = getS3Client()
	}

	input := &s3.ListObjectsInput{
		Bucket: aws.String(os.Getenv("BUCKET_NAME")),
	}
	res, err := s3Client.ListObjects(ctx, input)
	if err != nil {
		return ""
	}
	for _, v := range res.Contents {
		if strings.HasPrefix(stringValue(v.Key), bucketResultPath + "/" + getForecastId(id)) && strings.HasSuffix(stringValue(v.Key), "part0.csv") {
			return stringValue(v.Key)
		}
	}
	return ""
}

func uploadData(ctx context.Context, id string, values []float64) error {
	t := time.Now()
	stringData := "item_id,timestamp,target_value\n"
	contentType := "text/csv"
	filename := getForecastId(id) + ".csv"
	for i, v := range values {
		t_ := t.AddDate(0, 0, i - len(values))
		stringData += "v," + t_.Format(layout3) + "," + strconv.FormatFloat(v, 'f', -1, 64) + "\n"
	}
	if s3Client == nil {
		s3Client = getS3Client()
	}
	input := &s3.PutObjectInput{
		ACL: stypes.ObjectCannedACLPrivate,
		Bucket: aws.String(os.Getenv("BUCKET_NAME")),
		Key: aws.String(bucketPath + "/" + filename),
		Body: bytes.NewReader([]byte(stringData)),
		ContentType: aws.String(contentType),
	}
	_, err := s3Client.PutObject(ctx, input)
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
	err := uploadData(ctx, progressId, values)
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
	log.Printf("%+v\n", res.Status)
	if res.Status == nil {
		// CreateDatasetImportJob
		ds := getDataset(ctx, id)
		if ds.DatasetArn == nil {
			return "", fmt.Errorf("Error: %s", "No Dataset.")
		}
		path := "s3://" + os.Getenv("BUCKET_NAME") + "/" + bucketPath + "/" + getForecastId(id) + ".csv"
		_, err := createDatasetImportJob(ctx, id, stringValue(ds.DatasetArn), path, os.Getenv("FORECAST_ROLE_ARN"))
		if err != nil {
			log.Print(err)
			return "", err
		}
		return "Start", nil
	}
	return stringValue(res.Status), nil
}

func checkPredictor(ctx context.Context, id string)(string, error) {
	// GetPredictor
	res := getPredictor(ctx, id)
	if res.Status == nil {
		// CreatePredictor
		dsg := getDatasetGroup(ctx, id)
		if dsg.DatasetGroupArn == nil {
			return "", fmt.Errorf("Error: %s", "No DatasetGroup.")
		}
		_, err := createPredictor(ctx, id, stringValue(dsg.DatasetGroupArn))
		if err != nil {
			log.Print(err)
			return "", err
		}
		return "Start", nil
	}
	return stringValue(res.Status), nil
}

func checkForecast(ctx context.Context, id string)(string, error) {
	// GetForecast
	res := getForecast(ctx, id)
	if res.Status == nil {
		// CreateForecast
		pre := getPredictor(ctx, id)
		if pre.PredictorArn == nil {
			return "", fmt.Errorf("Error: %s", "No Predictor.")
		}
		_, err := createForecast(ctx, id, stringValue(pre.PredictorArn))
		if err != nil {
			log.Print(err)
			return "", err
		}
		return "Start", nil
	}
	return stringValue(res.Status), nil
}

func checkExport(ctx context.Context, id string)(string, error) {
	// GetForecastExportJob
	res := getForecastExportJob(ctx, id)
	if res.Status == nil {
		// CreateForecastExportJob
		fct := getForecast(ctx, id)
		if fct.ForecastArn == nil {
			return "", fmt.Errorf("Error: %s", "No Forecast.")
		}
		path := "s3://" + os.Getenv("BUCKET_NAME") + "/" + bucketResultPath + "/" + getForecastId(id)
		_, err := createForecastExportJob(ctx, id, stringValue(fct.ForecastArn), path, os.Getenv("FORECAST_ROLE_ARN"))
		if err != nil {
			log.Print(err)
			return "", err
		}
		return "Start", nil
	}
	return stringValue(res.Status), nil
}

func getResult(ctx context.Context, id string)(string, error) {
	resultData := ""
	objectKey := getObjectKey(ctx, id)
	if len(objectKey) == 0 {
		return "", fmt.Errorf("Error: %s", "No ObjectKey.")
	}
	if s3Client == nil {
		s3Client = getS3Client()
	}
	input := &s3.GetObjectInput{
		Bucket: aws.String(os.Getenv("BUCKET_NAME")),
		Key:    aws.String(objectKey),
	}
	res, err := s3Client.GetObject(ctx, input)
	if err != nil {
		return "", err
	}

	rc := res.Body
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

func getS3Client() *s3.Client {
	if cfg.Region != os.Getenv("REGION") {
		cfg = getConfig()
	}
	return s3.NewFromConfig(cfg)
}

func getForecastClient() *forecast.Client {
	if cfg.Region != os.Getenv("REGION") {
		cfg = getConfig()
	}
	return forecast.NewFromConfig(cfg)
}

func getConfig() aws.Config {
	var err error
	newConfig, err := config.LoadDefaultConfig()
	newConfig.Region = os.Getenv("REGION")
	if err != nil {
		log.Print(err)
	}
	return newConfig
}

func stringValue(i interface{}) string {
	var buf bytes.Buffer
	strVal(reflect.ValueOf(i), 0, &buf)
	res := buf.String()
	return res[1:len(res) - 1]
}

func strVal(v reflect.Value, indent int, buf *bytes.Buffer) {
	for v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.Struct:
		buf.WriteString("{\n")
		for i := 0; i < v.Type().NumField(); i++ {
			ft := v.Type().Field(i)
			fv := v.Field(i)
			if ft.Name[0:1] == strings.ToLower(ft.Name[0:1]) {
				continue // ignore unexported fields
			}
			if (fv.Kind() == reflect.Ptr || fv.Kind() == reflect.Slice) && fv.IsNil() {
				continue // ignore unset fields
			}
			buf.WriteString(strings.Repeat(" ", indent+2))
			buf.WriteString(ft.Name + ": ")
			if tag := ft.Tag.Get("sensitive"); tag == "true" {
				buf.WriteString("<sensitive>")
			} else {
				strVal(fv, indent+2, buf)
			}
			buf.WriteString(",\n")
		}
		buf.WriteString("\n" + strings.Repeat(" ", indent) + "}")
	case reflect.Slice:
		nl, id, id2 := "", "", ""
		if v.Len() > 3 {
			nl, id, id2 = "\n", strings.Repeat(" ", indent), strings.Repeat(" ", indent+2)
		}
		buf.WriteString("[" + nl)
		for i := 0; i < v.Len(); i++ {
			buf.WriteString(id2)
			strVal(v.Index(i), indent+2, buf)
			if i < v.Len()-1 {
				buf.WriteString("," + nl)
			}
		}
		buf.WriteString(nl + id + "]")
	case reflect.Map:
		buf.WriteString("{\n")
		for i, k := range v.MapKeys() {
			buf.WriteString(strings.Repeat(" ", indent+2))
			buf.WriteString(k.String() + ": ")
			strVal(v.MapIndex(k), indent+2, buf)
			if i < v.Len()-1 {
				buf.WriteString(",\n")
			}
		}
		buf.WriteString("\n" + strings.Repeat(" ", indent) + "}")
	default:
		format := "%v"
		switch v.Interface().(type) {
		case string:
			format = "%q"
		}
		fmt.Fprintf(buf, format, v.Interface())
	}
}

func main() {
	lambda.Start(HandleRequest)
}
