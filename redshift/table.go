package redshift

import (
	"fmt"
	_ "github.com/lib/pq"
	bq "google.golang.org/api/bigquery/v2"
	"strings"
)

const (
	SMALLINT  = iota
	INTEGER   = iota
	BIGINT    = iota
	DECIMAL   = iota
	DOUBLE    = iota
	BOOLEAN   = iota
	CHAR      = iota
	VARCHAR   = iota
	DATE      = iota
	TIMESTAMP = iota
)

type Column struct {
	Name string
	Type int
}

func (c *Column) String() string {
	return c.Name
}

type TableSchema struct {
	Columns []*Column
}

func (in *TableSchema) ToBigQuerySchema() (*bq.TableSchema, error) {
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

func bigqueryColumnType(t int) (string, error) {
	if t == SMALLINT || t == INTEGER || t == BIGINT {
		return "INTEGER", nil
	}
	if t == DECIMAL || t == DOUBLE {
		return "FLOAT", nil
	}
	if t == BOOLEAN {
		return "BOOLEAN", nil
	}
	if t == CHAR || t == VARCHAR {
		return "STRING", nil
	}
	if t == DATE {
		return "STRING", nil
	}
	if t == TIMESTAMP {
		return "TIMESTAMP", nil
	}

	return "", fmt.Errorf("unexpected column type: %d", t)
}

func (t *TableSchema) String() string {
	return fmt.Sprintf("%+v", t.Columns)
}

func (c *Client) ExtractSchema(table string) (*TableSchema, error) {
	rows, err := c.query(fmt.Sprintf("SELECT \"column\", \"type\" FROM pg_table_def WHERE tablename = '%s'", table))
	if err != nil {
		return nil, err
	}

	cols := make([]*Column, 0)
	defer rows.Close()
	for rows.Next() {
		var name string
		var t string
		err = rows.Scan(&name, &t)
		if err != nil {
			return nil, err
		}
		colType, err := columnType(t)
		if err != nil {
			return nil, err
		}
		cols = append(cols, &Column{name, colType})
	}

	return &TableSchema{cols}, nil
}

func columnType(t string) (int, error) {
	if strings.HasPrefix(t, "timestamp") {
		return TIMESTAMP, nil
	}
	if strings.HasPrefix(t, "text") {
		return VARCHAR, nil
	}
	if strings.HasPrefix(t, "character varying") {
		return VARCHAR, nil
	}
	if strings.HasPrefix(t, "character") {
		return CHAR, nil
	}
	if strings.HasPrefix(t, "date") {
		return DATE, nil
	}
	if strings.HasPrefix(t, "double") {
		return DOUBLE, nil
	}
	if strings.HasPrefix(t, "integer") {
		return INTEGER, nil
	}
	if strings.HasPrefix(t, "bigint") {
		return INTEGER, nil
	}
	if strings.HasPrefix(t, "smallint") {
		return SMALLINT, nil
	}
	if strings.HasPrefix(t, "boolean") {
		return BOOLEAN, nil
	}
	if strings.HasPrefix(t, "numeric") {
		return DECIMAL, nil
	}
	return 0, fmt.Errorf("unexpected col type: %s", t)
}
