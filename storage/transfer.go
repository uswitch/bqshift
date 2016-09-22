package storage

import (
	"encoding/json"
	"fmt"
	bq "github.com/uswitch/bqshift/bigquery"
	"github.com/uswitch/bqshift/redshift"
	"github.com/uswitch/bqshift/util"
	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
	transfer "google.golang.org/api/storagetransfer/v1"
	"log"
	"time"
)

type Client struct {
	service  *transfer.Service
	config   *bq.Configuration
	s3config *redshift.S3Configuration
}

func NewClient(config *bq.Configuration, s3 *redshift.S3Configuration) (*Client, error) {
	ctx := context.Background()
	client, err := google.DefaultClient(ctx, transfer.CloudPlatformScope)
	if err != nil {
		return nil, err
	}
	svc, err := transfer.New(client)
	if err != nil {
		return nil, err
	}

	c := &Client{svc, config, s3}
	return c, nil
}

func filterString(projectId, jobName string) (string, error) {
	m := make(map[string]interface{})
	m["project_id"] = projectId
	names := make([]string, 1)
	names[0] = jobName
	m["job_names"] = names
	bs, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	return string(bs), nil
}

func (c *Client) blockForJobCompletion(createdJob *transfer.TransferJob) error {
	filter, err := filterString(createdJob.ProjectId, createdJob.Name)
	if err != nil {
		fmt.Errorf("error creating list filter: %s", err.Error())
	}
	ticks := time.Tick(30 * time.Second)

	for _ = range ticks {
		resp, err := util.RetryOp(func() (interface{}, error) {
			return c.service.TransferOperations.List("transferOperations").Filter(filter).Do()
		})

		if err != nil {
			return fmt.Errorf("error listing operations: %s", err.Error())
		}

		ops := resp.(*transfer.ListOperationsResponse)
		if len(ops.Operations) != 1 {
			log.Println("waiting another 30s for transfer operation.")
			continue
		}

		op := ops.Operations[0]
		if op.Done {
			if op.Error != nil {
				return fmt.Errorf("transfer operation failed: %s", op.Error.Message)
			}
			return nil
		} else {
			log.Println("transfer in progress. waiting 30s.")
		}
	}

	return nil
}

type StoredResult struct {
	BucketName string
	Prefix     string
}

func (c *Client) TransferToCloudStorage(source *redshift.UnloadResult) (*StoredResult, error) {
	startTime := time.Now().Add(1 * time.Minute).UTC()

	startDate := &transfer.Date{
		Day:   int64(startTime.Day()),
		Month: int64(startTime.Month()),
		Year:  int64(startTime.Year()),
	}

	destinationBucket := source.Bucket

	job := &transfer.TransferJob{
		Description: fmt.Sprintf("bqshift %s", source.ObjectPrefix),
		Status:      "ENABLED",
		ProjectId:   c.config.ProjectID,
		Schedule: &transfer.Schedule{
			ScheduleEndDate:   startDate,
			ScheduleStartDate: startDate,
			StartTimeOfDay: &transfer.TimeOfDay{
				Hours:   int64(startTime.Hour()),
				Minutes: int64(startTime.Minute()),
			},
		},
		TransferSpec: &transfer.TransferSpec{
			TransferOptions: &transfer.TransferOptions{
				DeleteObjectsFromSourceAfterTransfer:  true,
				OverwriteObjectsAlreadyExistingInSink: true,
			},
			AwsS3DataSource: &transfer.AwsS3Data{
				AwsAccessKey: &transfer.AwsAccessKey{
					AccessKeyId:     c.s3config.Credentials.AccessKey,
					SecretAccessKey: c.s3config.Credentials.SecretKey,
				},
				BucketName: source.Bucket,
			},
			GcsDataSink: &transfer.GcsData{
				BucketName: destinationBucket,
			},
			ObjectConditions: &transfer.ObjectConditions{
				IncludePrefixes: []string{source.ObjectPrefix},
			},
		},
	}

	created, err := util.RetryOp(func() (interface{}, error) {
		return c.service.TransferJobs.Create(job).Do()
	})
	if err != nil {
		return nil, err
	}

	log.Println("transfer requested, this may take a while.")
	err = c.blockForJobCompletion(created.(*transfer.TransferJob))
	if err != nil {
		return nil, err
	}

	return &StoredResult{BucketName: destinationBucket, Prefix: source.ObjectPrefix}, nil
}
