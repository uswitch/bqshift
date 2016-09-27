package bigquery

import (
	"fmt"
	bq "google.golang.org/api/bigquery/v2"
	"time"
)

type TableReference struct {
	ProjectID string
	DatasetID string
	TableID   string
	DayShard  *time.Time
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
	r := &bq.TableReference{
		DatasetId: ref.DatasetID,
		ProjectId: ref.ProjectID,
		TableId:   ref.TableID,
	}
	if ref.DayShard == nil {
		return r
	}
	r.TableId = fmt.Sprintf("%s_%s", r.TableId, bqfmt(*ref.DayShard))
	return r
}

func (ref *TableReference) String() string {
	return fmt.Sprintf("%s:%s.%s", ref.ProjectID, ref.DatasetID, ref.TableID)
}
