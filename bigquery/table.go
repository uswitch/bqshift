package bigquery

import (
	"fmt"
	"github.com/uswitch/bqshift/util"
	bq "google.golang.org/api/bigquery/v2"
	"log"
	"time"
)

type TableReference struct {
	ProjectID    string
	DatasetID    string
	TableID      string
	DayPartition *time.Time
}

func (c *Client) EnsureTableExists(ref *TableReference, partition bool) error {
	resp, err := util.RetryOp(func() (interface{}, error) {
		return c.service.Tables.List(ref.ProjectID, ref.DatasetID).Do()
	})

	if err != nil {
		return err
	}

	list := resp.(*bq.TableList)
	for _, table := range list.Tables {
		if table.TableReference.TableId == ref.TableID {
			return nil
		}
	}

	newTable := &bq.Table{
		Description:    "bqshift created table",
		TableReference: ref.ToGoogleReference(),
	}

	if partition {
		newTable.TimePartitioning = &bq.TimePartitioning{
			Type: "DAY",
		}
	}
	_, err = util.RetryOp(func() (interface{}, error) {
		return c.service.Tables.Insert(ref.ProjectID, ref.DatasetID, newTable).Do()
	})

	if err != nil {
		return err
	}

	log.Println("created bigquery table.")

	return nil
}

type DatasetReference struct {
	ProjectID string
	DatasetID string
}

func (ref *TableReference) DatasetReference() *DatasetReference {
	return &DatasetReference{ref.ProjectID, ref.DatasetID}
}

func NewTableReference(projectId, dataset, table string) *TableReference {
	return &TableReference{
		ProjectID: projectId,
		DatasetID: dataset,
		TableID:   table,
	}
}

func bqfmt(t time.Time) string {
	return t.Format("20060102")
}

func (ref *TableReference) ToGoogleReference() *bq.TableReference {
	return &bq.TableReference{
		DatasetId: ref.DatasetID,
		ProjectId: ref.ProjectID,
		TableId:   ref.TableID,
	}
}

func (ref *TableReference) ToPartitionedReference() *bq.TableReference {
	return &bq.TableReference{
		DatasetId: ref.DatasetID,
		ProjectId: ref.ProjectID,
		TableId:   fmt.Sprintf("%s$%s", ref.TableID, bqfmt(*ref.DayPartition)),
	}
}

func (ref *TableReference) String() string {
	return fmt.Sprintf("%s:%s.%s", ref.ProjectID, ref.DatasetID, ref.TableID)
}
