package bigquery

import (
	"fmt"
	"github.com/uswitch/bqshift/redshift"
	bq "google.golang.org/api/bigquery/v2"
	"log"
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

func (c *Client) LoadTable(spec *LoadSpec) error {
	pattern := sourcePattern(spec.BucketName, spec.ObjectPrefix)

	log.Printf("loading %s into %s\n", pattern, spec.TableReference)

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

	_, err := c.service.Jobs.Insert(spec.TableReference.ProjectID, &bq.Job{Configuration: config}).Do()
	if err != nil {
		return err
	}

	return nil
}
