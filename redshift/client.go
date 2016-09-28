package redshift

import (
	"database/sql"
	"fmt"
)

type Client struct {
	aws *AWSConfiguration
	db  *sql.DB
}

func NewClient(config *AWSConfiguration) (*Client, error) {
	client := &Client{config, nil}
	client.Connect()

	return client, nil
}

func (c *Client) Connect() error {
	db, err := sql.Open("postgres", c.aws.Redshift.URLString())
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

func (c *Client) doUnload(source *RedshiftSource) (*UnloadResult, error) {
	return newUnloadOperation(c, c.aws, source).execute()
}

func (c *Client) Unload(table string, partition *DatePartition) (*UnloadResult, error) {
	schema, err := c.ExtractSchema(table)
	if err != nil {
		return nil, fmt.Errorf("error extracting table schema: %s", err.Error())
	}
	source := &RedshiftSource{
		Table:     table,
		Schema:    schema,
		Partition: partition,
	}
	return c.doUnload(source)
}
