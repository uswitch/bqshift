package bigquery

import (
	"fmt"
	"github.com/uswitch/bqshift/redshift"
	bq "google.golang.org/api/bigquery/v2"
	"log"
	"time"
)

type TableRef struct {
	ProjectID string
	DatasetID string
	TableID   string
}

func (ref *TableRef) ToGoogleReference() *bq.TableReference {
	return &bq.TableReference{
		DatasetId: ref.DatasetID,
		ProjectId: ref.ProjectID,
		TableId:   ref.TableID,
	}
}

func (ref *TableRef) String() string {
	return fmt.Sprintf("%s:%s.%s", ref.ProjectID, ref.DatasetID, ref.TableID)
}

func TableReference(projectId, datasetId, tableId string) *TableRef {
	return &TableRef{projectId, datasetId, tableId}
}

func sourcePattern(bucket, prefix string) string {
	return fmt.Sprintf("gs://%s/%s/*", bucket, prefix)
}

type LoadSpec struct {
	TableReference *TableRef
	BucketName     string
	ObjectPrefix   string
	Overwrite      bool
	Schema         *bq.TableSchema
}

type LoadCompleted struct {
	Error error
}

const StateDone = "DONE"

func (c *Client) reportJobCompletion(projectId string, createdJob *bq.Job, finished chan *LoadCompleted) {
	for {
		job, err := c.service.Jobs.Get(projectId, createdJob.JobReference.JobId).Do()

		if err != nil {
			finished <- &LoadCompleted{err}
			return
		}

		if job.Status.State == StateDone {
			if job.Status.ErrorResult == nil {
				finished <- &LoadCompleted{}
			} else {
				err = fmt.Errorf("Load job failed. Location: %s; Reason: %s. %s", job.Status.ErrorResult.Location, job.Status.ErrorResult.Reason, job.Status.ErrorResult.Message)
				finished <- &LoadCompleted{err}
			}

			return
		}

		log.Printf("load status %s. waiting 30s.\n", job.Status.State)

		time.Sleep(30 * time.Second)
	}
}

func (c *Client) LoadTable(spec *LoadSpec) (<-chan *LoadCompleted, error) {
	pattern := sourcePattern(spec.BucketName, spec.ObjectPrefix)

	config := &bq.JobConfiguration{
		Load: &bq.JobConfigurationLoad{
			CreateDisposition:   "CREATE_IF_NEEDED",
			WriteDisposition:    "WRITE_EMPTY",
			DestinationTable:    spec.TableReference.ToGoogleReference(),
			FieldDelimiter:      redshift.DefaultDelimiter(),
			IgnoreUnknownValues: false,
			SourceFormat:        "CSV",
			SourceUris:          []string{pattern},
		},
	}

	if spec.Overwrite {
		config.Load.WriteDisposition = "WRITE_TRUNCATE"
	}

	job, err := c.service.Jobs.Insert(spec.TableReference.ProjectID, &bq.Job{Configuration: config}).Do()
	if err != nil {
		return nil, err
	}

	done := make(chan *LoadCompleted)
	go c.reportJobCompletion(spec.TableReference.ProjectID, job, done)
	return done, nil
}
