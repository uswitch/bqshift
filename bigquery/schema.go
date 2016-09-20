package bigquery

import (
	"fmt"
	"github.com/uswitch/bqshift/redshift"
	bq "google.golang.org/api/bigquery/v2"
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

func bigqueryColumnType(t int) (string, error) {
	if t == redshift.SMALLINT || t == redshift.INTEGER || t == redshift.BIGINT {
		return "INTEGER", nil
	}
	if t == redshift.DECIMAL || t == redshift.DOUBLE {
		return "FLOAT", nil
	}
	if t == redshift.BOOLEAN {
		return "BOOLEAN", nil
	}
	if t == redshift.CHAR || t == redshift.VARCHAR {
		return "STRING", nil
	}
	if t == redshift.DATE {
		return "STRING", nil
	}
	if t == redshift.TIMESTAMP {
		return "TIMESTAMP", nil
	}

	return "", fmt.Errorf("unexpected column type: %d", t)
}

func translateSchema(in *redshift.TableSchema) (*bq.TableSchema, error) {
	fields := make([]*bq.TableFieldSchema, len(in.Columns))

	for _, column := range in.Columns {
		t, err := bigqueryColumnType(column.Type)
		if err != nil {
			return nil, fmt.Errorf("couldn't map column %s: %s", column.Name, err.Error())
		}
		field := &bq.TableFieldSchema{
			Name: column.Name,
			Type: t,
		}
		fields = append(fields, field)
	}

	return &bq.TableSchema{
		Fields: fields,
	}, nil
}

func (c *Client) CreateTable(ref *TableRef, schema *redshift.TableSchema) error {
	bqSchema, err := translateSchema(schema)
	if err != nil {
		return err
	}

	table := &bq.Table{
		Description:    "Replicated with bqshift",
		Schema:         bqSchema,
		TableReference: ref.ToGoogleReference(),
	}
	fmt.Println("creating table", ref)

	_, err = c.service.Tables.Insert(ref.ProjectID, ref.DatasetID, table).Do()
	if err != nil {
		return err
	}

	return nil
}

func sourcePattern(bucket, prefix string) string {
	return fmt.Sprintf("gs://%s/%s/*", bucket, prefix)
}

func (c *Client) LoadTable(ref *TableRef, bucket, prefix string) error {
	pattern := sourcePattern(bucket, prefix)

	fmt.Printf("loading %s into %s\n", pattern, ref)

	config := &bq.JobConfiguration{
		Load: &bq.JobConfigurationLoad{
			CreateDisposition:   "CREATE_NEVER",
			WriteDisposition:    "WRITE_EMPTY",
			DestinationTable:    ref.ToGoogleReference(),
			FieldDelimiter:      redshift.DefaultDelimiter(),
			IgnoreUnknownValues: false,
			SourceFormat:        "CSV",
			SourceUris:          []string{pattern},
		},
	}
	_, err := c.service.Jobs.Insert(ref.ProjectID, &bq.Job{Configuration: config}).Do()
	if err != nil {
		return err
	}

	return nil
}
