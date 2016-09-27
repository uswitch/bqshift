package bigquery

import (
	"fmt"
	"github.com/uswitch/bqshift/redshift"
	"github.com/uswitch/bqshift/util"
	bq "google.golang.org/api/bigquery/v2"
	"log"
	"time"
)

func sourcePattern(bucket, prefix string) string {
	return fmt.Sprintf("gs://%s/%s/*", bucket, prefix)
}

type LoadSpec struct {
	TableReference *TableReference
	BucketName     string
	ObjectPrefix   string
	Overwrite      bool
	Schema         *bq.TableSchema
	Partitioned    bool
}

type LoadCompleted struct {
	Error error
}

const StateDone = "DONE"

func (c *Client) blockForJobCompletion(projectId string, createdJob *bq.Job) error {
	for {
		resp, err := util.RetryOp(func() (interface{}, error) {
			return c.service.Jobs.Get(projectId, createdJob.JobReference.JobId).Do()
		})

		if err != nil {
			return err
		}

		job := resp.(*bq.Job)

		if job.Status.State == StateDone {
			if job.Status.ErrorResult == nil {
				return nil
			} else {
				return fmt.Errorf("Load job failed. Location: %s; Reason: %s. %s", job.Status.ErrorResult.Location, job.Status.ErrorResult.Reason, job.Status.ErrorResult.Message)
			}
		}

		log.Printf("load status %s. waiting 30s.\n", job.Status.State)
		time.Sleep(30 * time.Second)
	}
}

func (c *Client) LoadTable(spec *LoadSpec) error {
	pattern := sourcePattern(spec.BucketName, spec.ObjectPrefix)

	load := &bq.JobConfigurationLoad{
		CreateDisposition:   "CREATE_IF_NEEDED",
		WriteDisposition:    "WRITE_EMPTY",
		FieldDelimiter:      redshift.DefaultDelimiter(),
		IgnoreUnknownValues: false,
		SourceFormat:        "CSV",
		SourceUris:          []string{pattern},
		Schema:              spec.Schema,
	}
	if spec.Partitioned {
		load.DestinationTable = spec.TableReference.ToPartitionedReference()
	} else {
		load.DestinationTable = spec.TableReference.ToGoogleReference()
	}

	config := &bq.JobConfiguration{
		Load: load,
	}

	if spec.Overwrite {
		config.Load.WriteDisposition = "WRITE_TRUNCATE"
	}

	resp, err := util.RetryOp(func() (interface{}, error) {
		return c.service.Jobs.Insert(spec.TableReference.ProjectID, &bq.Job{Configuration: config}).Do()
	})

	if err != nil {
		return err
	}

	job := resp.(*bq.Job)
	log.Println("created load job, waiting for completion.")

	return c.blockForJobCompletion(spec.TableReference.ProjectID, job)
}
