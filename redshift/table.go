package redshift

import (
	"fmt"
	_ "github.com/lib/pq"
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
