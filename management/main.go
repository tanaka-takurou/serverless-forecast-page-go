package main

import (
	"log"
	"flag"
	"time"
	"bytes"
	"strconv"
	"encoding/json"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/forecastservice"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

const layout         string = "2006-01-02 15:04"
const layout2        string = "20060102150405"
const layout3        string = "2006-01-02 00:00:00"
const bucketRegion   string = "ap-northeast-1"
const forecastRegion string = "ap-northeast-1"

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

func createDatasetGroup(name string) error {
	svc := getForecastservice()

	input := &forecastservice.CreateDatasetGroupInput{
		DatasetGroupName: aws.String(name),
		Domain: aws.String("CUSTOM"),
	}
	res, err := svc.CreateDatasetGroup(input)
	if err != nil {
		return err
	}
	log.Println(aws.StringValue(res.DatasetGroupArn))
	return nil
}

func createDataset(name string) error {
	svc := getForecastservice()

	input := &forecastservice.CreateDatasetInput{
		DatasetName: aws.String(name),
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
		return err
	}
	log.Println(aws.StringValue(res.DatasetArn))
	return nil
}

func createDatasetImportJob(name string, datasetArn string, path string, roleArn string) error {
	svc := getForecastservice()

	input := &forecastservice.CreateDatasetImportJobInput{
		DatasetImportJobName: aws.String(name),
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
		return err
	}
	log.Println(aws.StringValue(res.DatasetImportJobArn))
	return nil
}

func createForecast(name string, predictorArn string) error {
	svc := getForecastservice()

	input := &forecastservice.CreateForecastInput{
		ForecastName: aws.String(name),
		PredictorArn: aws.String(predictorArn),
	}
	res, err := svc.CreateForecast(input)
	if err != nil {
		return err
	}
	log.Println(aws.StringValue(res.ForecastArn))
	return nil
}

func createForecastExportJob(name string, forecastArn string, path string, roleArn string) error {
	svc := getForecastservice()

	input := &forecastservice.CreateForecastExportJobInput{
		ForecastExportJobName: aws.String(name),
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
		return err
	}
	log.Println(aws.StringValue(res.ForecastExportJobArn))
	return nil
}

func createPredictor(name string, datasetGroupArn string) error {
	svc := getForecastservice()

	input := &forecastservice.CreatePredictorInput{
		PredictorName: aws.String(name),
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
		return err
	}
	log.Println(aws.StringValue(res.PredictorArn))
	return nil
}

func listDatasetGroups() error {
	svc := getForecastservice()

	input := &forecastservice.ListDatasetGroupsInput{}
	res, err := svc.ListDatasetGroups(input)
	if err != nil {
		return err
	}
	for _, v := range res.DatasetGroups {
		log.Println("[" + aws.StringValue(v.DatasetGroupName) + "] (" + aws.TimeValue(v.CreationTime).Format(layout) + ")" )
		log.Println("[arn] " + aws.StringValue(v.DatasetGroupArn) + "\n")
	}
	return nil
}

func listDatasets() error {
	svc := getForecastservice()

	input := &forecastservice.ListDatasetsInput{}
	res, err := svc.ListDatasets(input)
	if err != nil {
		return err
	}
	for _, v := range res.Datasets {
		log.Println("[" + aws.StringValue(v.DatasetName) + "] (" + aws.TimeValue(v.CreationTime).Format(layout) + ")" )
		log.Println("[arn] " + aws.StringValue(v.DatasetArn) + "\n")
	}
	return nil
}

func listDatasetImportJobs() error {
	svc := getForecastservice()

	input := &forecastservice.ListDatasetImportJobsInput{}
	res, err := svc.ListDatasetImportJobs(input)
	if err != nil {
		return err
	}
	for _, v := range res.DatasetImportJobs {
		log.Println("[" + aws.StringValue(v.DatasetImportJobName) + "] (" + aws.TimeValue(v.CreationTime).Format(layout) + ")" )
		log.Println("[arn] " + aws.StringValue(v.DatasetImportJobArn))
		log.Println("[status] " + aws.StringValue(v.Status) + "\n")
	}
	return nil
}

func listForecasts() error {
	svc := getForecastservice()

	input := &forecastservice.ListForecastsInput{}
	res, err := svc.ListForecasts(input)
	if err != nil {
		return err
	}
	for _, v := range res.Forecasts {
		log.Println("[" + aws.StringValue(v.ForecastName) + "] (" + aws.TimeValue(v.CreationTime).Format(layout) + ")" )
		log.Println("[datasetGroupArn] " + aws.StringValue(v.DatasetGroupArn))
		log.Println("[forecastArn] " + aws.StringValue(v.ForecastArn))
		log.Println("[predictorArn] " + aws.StringValue(v.PredictorArn))
		log.Println("[status] " + aws.StringValue(v.Status) + "\n")
	}
	return nil
}

func listForecastExportJobs() error {
	svc := getForecastservice()

	input := &forecastservice.ListForecastExportJobsInput{}
	res, err := svc.ListForecastExportJobs(input)
	if err != nil {
		return err
	}
	for _, v := range res.ForecastExportJobs {
		log.Println("[" + aws.StringValue(v.ForecastExportJobName) + "] (" + aws.TimeValue(v.CreationTime).Format(layout) + ")" )
		log.Println("[arn] " + aws.StringValue(v.ForecastExportJobArn))
		log.Println("[status] " + aws.StringValue(v.Status) + "\n")
	}
	return nil
}

func listPredictors() error {
	svc := getForecastservice()

	input := &forecastservice.ListPredictorsInput{}
	res, err := svc.ListPredictors(input)
	if err != nil {
		return err
	}
	for _, v := range res.Predictors {
		log.Println("[" + aws.StringValue(v.PredictorName) + "] (" + aws.TimeValue(v.CreationTime).Format(layout) + ")" )
		log.Println("[predictorArn] " + aws.StringValue(v.PredictorArn))
		log.Println("[datasetGroupArn] " + aws.StringValue(v.DatasetGroupArn))
		log.Println("[status] " + aws.StringValue(v.Status) + "\n")
	}
	return nil
}

func describeDatasetGroup(datasetGroupArn string) error {
	svc := getForecastservice()

	input := &forecastservice.DescribeDatasetGroupInput{
		DatasetGroupArn: aws.String(datasetGroupArn),
	}
	res, err := svc.DescribeDatasetGroup(input)
	if err != nil {
		return err
	}
	datasetArns := ""
	for _, v := range res.DatasetArns {
		datasetArns += aws.StringValue(v) + " , "
	}
	if len(datasetArns) > 0 {
		datasetArns = datasetArns[:len(datasetArns) - 3]
	}
	log.Println("[" + aws.StringValue(res.DatasetGroupName) + "] (" + aws.TimeValue(res.LastModificationTime).Format(layout) + ")" )
	log.Println("[datasetArns] " + datasetArns )
	log.Println("[status] " + aws.StringValue(res.Status) )
	return nil
}

func describeDataset(datasetArn string) error {
	svc := getForecastservice()

	input := &forecastservice.DescribeDatasetInput{
		DatasetArn: aws.String(datasetArn),
	}
	res, err := svc.DescribeDataset(input)
	if err != nil {
		return err
	}
	log.Println("[" + aws.StringValue(res.DatasetName) + "] (" + aws.TimeValue(res.LastModificationTime).Format(layout) + ")" )
	log.Println("[status] " + aws.StringValue(res.Status) )
	return nil
}

func describeDatasetImportJob(datasetImportJobArn string) error {
	svc := getForecastservice()

	input := &forecastservice.DescribeDatasetImportJobInput{
		DatasetImportJobArn: aws.String(datasetImportJobArn),
	}
	res, err := svc.DescribeDatasetImportJob(input)
	if err != nil {
		return err
	}
	log.Println("[" + aws.StringValue(res.DatasetImportJobName) + "] (" + aws.TimeValue(res.LastModificationTime).Format(layout) + ")" )
	log.Println("[status] " + aws.StringValue(res.Status) )
	return nil
}

func describePredictor(predictorArn string) error {
	svc := getForecastservice()

	input := &forecastservice.DescribePredictorInput{
		PredictorArn: aws.String(predictorArn),
	}
	res, err := svc.DescribePredictor(input)
	if err != nil {
		return err
	}
	log.Println("[" + aws.StringValue(res.PredictorName) + "] (" + aws.TimeValue(res.LastModificationTime).Format(layout) + ")" )
	log.Println("[status] " + aws.StringValue(res.Status) )
	return nil
}

func describeForecast(forecastArn string) error {
	svc := getForecastservice()

	input := &forecastservice.DescribeForecastInput{
		ForecastArn: aws.String(forecastArn),
	}
	res, err := svc.DescribeForecast(input)
	if err != nil {
		return err
	}
	log.Println("[" + aws.StringValue(res.ForecastName) + "] (" + aws.TimeValue(res.LastModificationTime).Format(layout) + ")" )
	log.Println("[status] " + aws.StringValue(res.Status) )
	return nil
}

func describeForecastExportJob(forecastExportJobArn string) error {
	svc := getForecastservice()

	input := &forecastservice.DescribeForecastExportJobInput{
		ForecastExportJobArn: aws.String(forecastExportJobArn),
	}
	res, err := svc.DescribeForecastExportJob(input)
	if err != nil {
		return err
	}
	log.Println("[" + aws.StringValue(res.ForecastExportJobName) + "] (" + aws.TimeValue(res.LastModificationTime).Format(layout) + ")" )
	log.Println("[status] " + aws.StringValue(res.Status) )
	return nil
}

func deleteDatasetGroup(datasetGroupArn string) error {
	svc := getForecastservice()

	input := &forecastservice.DeleteDatasetGroupInput{
		DatasetGroupArn: aws.String(datasetGroupArn),
	}
	_, err := svc.DeleteDatasetGroup(input)
	if err != nil {
		return err
	}
	return nil
}

func deleteDataset(datasetArn string) error {
	svc := getForecastservice()

	input := &forecastservice.DeleteDatasetInput{
		DatasetArn: aws.String(datasetArn),
	}
	_, err := svc.DeleteDataset(input)
	if err != nil {
		return err
	}
	return nil
}

func deleteDatasetImportJob(datasetImportJobArn string) error {
	svc := getForecastservice()

	input := &forecastservice.DeleteDatasetImportJobInput{
		DatasetImportJobArn: aws.String(datasetImportJobArn),
	}
	_, err := svc.DeleteDatasetImportJob(input)
	if err != nil {
		return err
	}
	return nil
}

func deletePredictor(predictorArn string) error {
	svc := getForecastservice()

	input := &forecastservice.DeletePredictorInput{
		PredictorArn: aws.String(predictorArn),
	}
	_, err := svc.DeletePredictor(input)
	if err != nil {
		return err
	}
	return nil
}

func deleteForecast(forecastArn string) error {
	svc := getForecastservice()

	input := &forecastservice.DeleteForecastInput{
		ForecastArn: aws.String(forecastArn),
	}
	_, err := svc.DeleteForecast(input)
	if err != nil {
		return err
	}
	return nil
}

func deleteForecastExportJob(forecastExportJobArn string) error {
	svc := getForecastservice()

	input := &forecastservice.DeleteForecastExportJobInput{
		ForecastExportJobArn: aws.String(forecastExportJobArn),
	}
	_, err := svc.DeleteForecastExportJob(input)
	if err != nil {
		return err
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

func listBuckets() error {
	svc := getS3()

	input := &s3.ListBucketsInput{}
	res, err := svc.ListBuckets(input)
	if err != nil {
		return err
	}
	for _, v := range res.Buckets {
		log.Println("[" + aws.StringValue(v.Name) + "] (" + aws.TimeValue(v.CreationDate).Format(layout) + ")" )
	}
	return nil
}

func listObjects(bucketName string) error {
	svc := getS3()

	input := &s3.ListObjectsInput{
		Bucket: aws.String(bucketName),
	}
	res, err := svc.ListObjects(input)
	if err != nil {
		return err
	}
	for _, v := range res.Contents {
		log.Println("[" + aws.StringValue(v.Key) + "] (" + aws.TimeValue(v.LastModified).Format(layout) + ")" )
	}
	return nil
}

func createBucket(name string) error {
	svc := getS3()

	input := &s3.CreateBucketInput{
		Bucket: aws.String(name),
	}
	res, err := svc.CreateBucket(input)
	if err != nil {
		return err
	}
	log.Println(aws.StringValue(res.Location))
	return nil
}

func uploadData(bucketName string, jsonData string) error {
	t := time.Now()
	stringData := "item_id,timestamp,target_value\n"
	bucketPath := "csv"
	contentType := "text/csv"
	filename := t.Format(layout2) + ".csv"
	sess, err := getSession()
	if err != nil {
		log.Print(err)
		return err
	}
	var values []float64
	if err := json.Unmarshal([]byte(jsonData), &values); err != nil {
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

func main() {
	log.Println("[ Forecast Management ]")
	flag.Parse()

	switch flag.Arg(0) {
	case "createDatasetGroup":
		if len(flag.Args()) < 2 {
			log.Fatal("Error: No DatasetGroup Name.")
		} else if err := createDatasetGroup(flag.Arg(1)); err != nil {
			log.Fatal(err)
		}
	case "createDataset":
		if len(flag.Args()) < 2 {
			log.Fatal("Error: No Dataset Name.")
		} else if err := createDataset(flag.Arg(1)); err != nil {
			log.Fatal(err)
		}
	case "createDatasetImportJob":
		if len(flag.Args()) < 5 {
			log.Fatal("Error: No DatasetImportJob Name, DatasetArn, Path, RoleArn.")
		} else if err := createDatasetImportJob(flag.Arg(1), flag.Arg(2), flag.Arg(3), flag.Arg(4)); err != nil {
			log.Fatal(err)
		}
	case "createPredictor":
		if len(flag.Args()) < 3 {
			log.Fatal("Error: No Predictor Name, DatasetGroupArn.")
		} else if err := createPredictor(flag.Arg(1), flag.Arg(2)); err != nil {
			log.Fatal(err)
		}
	case "createForecast":
		if len(flag.Args()) < 3 {
			log.Fatal("Error: No Forecast Name, PredictorArn.")
		} else if err := createForecast(flag.Arg(1), flag.Arg(2)); err != nil {
			log.Fatal(err)
		}
	case "createForecastExportJob":
		if len(flag.Args()) < 5 {
			log.Fatal("Error: No ForecastExportJob Name, ForecastArn, Path, RoleArn.")
		} else if err := createForecastExportJob(flag.Arg(1), flag.Arg(2), flag.Arg(3), flag.Arg(4)); err != nil {
			log.Fatal(err)
		}
	case "listDatasetGroups":
		if err := listDatasetGroups(); err != nil {
			log.Fatal(err)
		}
	case "listDatasets":
		if err := listDatasets(); err != nil {
			log.Fatal(err)
		}
	case "listDatasetImportJobs":
		if err := listDatasetImportJobs(); err != nil {
			log.Fatal(err)
		}
	case "listPredictors":
		if err := listPredictors(); err != nil {
			log.Fatal(err)
		}
	case "listForecasts":
		if err := listForecasts(); err != nil {
			log.Fatal(err)
		}
	case "listForecastExportJobs":
		if err := listForecastExportJobs(); err != nil {
			log.Fatal(err)
		}
	case "describeDatasetGroup":
		if len(flag.Args()) < 2 {
			log.Fatal("Error: No DatasetGroupArn.")
		} else if err := describeDatasetGroup(flag.Arg(1)); err != nil {
			log.Fatal(err)
		}
	case "describeDataset":
		if len(flag.Args()) < 2 {
			log.Fatal("Error: No DatasetArn.")
		} else if err := describeDataset(flag.Arg(1)); err != nil {
			log.Fatal(err)
		}
	case "describeDatasetImportJob":
		if len(flag.Args()) < 2 {
			log.Fatal("Error: No DdatasetImportJobArn.")
		} else if err := describeDatasetImportJob(flag.Arg(1)); err != nil {
			log.Fatal(err)
		}
	case "describePredictor":
		if len(flag.Args()) < 2 {
			log.Fatal("Error: No PredictorArn.")
		} else if err := describePredictor(flag.Arg(1)); err != nil {
			log.Fatal(err)
		}
	case "describeForecast":
		if len(flag.Args()) < 2 {
			log.Fatal("Error: No ForecastArn.")
		} else if err := describeForecast(flag.Arg(1)); err != nil {
			log.Fatal(err)
		}
	case "describeForecastExportJob":
		if len(flag.Args()) < 2 {
			log.Fatal("Error: No ForecastExportJobArn.")
		} else if err := describeForecastExportJob(flag.Arg(1)); err != nil {
			log.Fatal(err)
		}
	case "deleteDatasetGroup":
		if len(flag.Args()) < 2 {
			log.Fatal("Error: No DatasetGroupArn.")
		} else if err := deleteDatasetGroup(flag.Arg(1)); err != nil {
			log.Fatal(err)
		}
	case "deleteDataset":
		if len(flag.Args()) < 2 {
			log.Fatal("Error: No DatasetArn.")
		} else if err := deleteDataset(flag.Arg(1)); err != nil {
			log.Fatal(err)
		}
	case "deleteDatasetImportJob":
		if len(flag.Args()) < 2 {
			log.Fatal("Error: No DdatasetImportJobArn.")
		} else if err := deleteDatasetImportJob(flag.Arg(1)); err != nil {
			log.Fatal(err)
		}
	case "deletePredictor":
		if len(flag.Args()) < 2 {
			log.Fatal("Error: No PredictorArn.")
		} else if err := deletePredictor(flag.Arg(1)); err != nil {
			log.Fatal(err)
		}
	case "deleteForecast":
		if len(flag.Args()) < 2 {
			log.Fatal("Error: No ForecastArn.")
		} else if err := deleteForecast(flag.Arg(1)); err != nil {
			log.Fatal(err)
		}
	case "deleteForecastExportJob":
		if len(flag.Args()) < 2 {
			log.Fatal("Error: No ForecastExportJobArn.")
		} else if err := deleteForecastExportJob(flag.Arg(1)); err != nil {
			log.Fatal(err)
		}
	case "updateDatasetGroup":
		if len(flag.Args()) < 3 {
			log.Fatal("Error: No DatasetArn, DatasetGroupArn.")
		} else if err := updateDatasetGroup(flag.Arg(1), flag.Arg(2)); err != nil {
			log.Fatal(err)
		}
	case "createBucket":
		if len(flag.Args()) < 2 {
			log.Fatal("Error: No Bucket Name.")
		} else if err := createBucket(flag.Arg(1)); err != nil {
			log.Fatal(err)
		}
	case "uploadData":
		if len(flag.Args()) < 3 {
			log.Fatal("Error: No BucketName, Data.")
		} else if err := uploadData(flag.Arg(1), flag.Arg(2)); err != nil {
			log.Fatal(err)
		}
	case "listBuckets":
		if err := listBuckets(); err != nil {
			log.Fatal(err)
		}
	case "listObjects":
		if len(flag.Args()) < 2 {
			log.Fatal("Error: No BucketName.")
		} else if err := listObjects(flag.Arg(1)); err != nil {
			log.Fatal(err)
		}
	default:
		log.Println("Error: Bad Command.")
		log.Println("forecast: {create}{DatasetGroup|Dataset|DatasetImportJob|Predictor|Forecast|ForecastExportJob}")
		log.Println("forecast: {list}{DatasetGroups|Datasets|DatasetImportJobs|Predictors|Forecasts|ForecastExportJobs}")
		log.Println("forecast: {describe}{DatasetGroup|Dataset|DatasetImportJob|Predictor|Forecast|ForecastExportJob}")
		log.Println("forecast: {delete}{DatasetGroup|Dataset|DatasetImportJob|Predictor|Forecast|ForecastExportJob}")
		log.Println("forecast: updateDatasetGroup")
		log.Println("s3: createBucket , uploadData , listBuckets , listObjects")
	}

}
