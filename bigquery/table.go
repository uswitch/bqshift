package bigquery

import (
	"fmt"
	bq "google.golang.org/api/bigquery/v2"
)

type TableReference struct {
	ProjectID string
	DatasetID string
	TableID   string
}

type DatasetReference struct {
	ProjectID string
	DatasetID string
}

func (ref *TableReference) DatasetReference() *DatasetReference {
	return &DatasetReference{ref.ProjectID, ref.DatasetID}
}

func NewTableReference(projectId, dataset, table string) *TableReference {
	return &TableReference{projectId, dataset, table}
}

func (ref *TableReference) ToGoogleReference() *bq.TableReference {
	return &bq.TableReference{
		DatasetId: ref.DatasetID,
		ProjectId: ref.ProjectID,
		TableId:   ref.TableID,
	}
}

func (ref *TableReference) String() string {
	return fmt.Sprintf("%s:%s.%s", ref.ProjectID, ref.DatasetID, ref.TableID)
}
