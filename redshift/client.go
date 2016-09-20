package redshift

import (
	"database/sql"
	"fmt"
)

type Client struct {
	redshift *RedshiftConfiguration
	s3       *S3Configuration
	db       *sql.DB
}

func NewClient(config *RedshiftConfiguration, s3 *S3Configuration) (*Client, error) {
	client := &Client{config, s3, nil}
	client.Connect()

	return client, nil
}

func (c *Client) Connect() error {
	db, err := sql.Open("postgres", c.redshift.URLString())
	if err != nil {
		return fmt.Errorf("error connecting to redshift: %s", err.Error())
	}
	c.db = db
	return nil
}

func (c *Client) Close() error {
	return c.db.Close()
}

func (c *Client) query(query string, args ...interface{}) (*sql.Rows, error) {
	return c.db.Query(query, args...)
}

func (c *Client) execute(statement string, args ...interface{}) (sql.Result, error) {
	return c.db.Exec(statement, args...)
}

func (c *Client) Unload(table string) (*UnloadResult, error) {
	schema, err := c.ExtractSchema(table)
	if err != nil {
		return nil, fmt.Errorf("error extracting table schema: %s", err.Error())
	}

	op := newUnloadOperation(c, c.s3, table, schema)
	return op.execute()
}
