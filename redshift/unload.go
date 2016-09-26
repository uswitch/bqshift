package redshift

import (
	"bytes"
	"fmt"
)

type unloadOperation struct {
	client *Client
	config *AWSConfiguration
	table  string
	schema *TableSchema
}

type UnloadResult struct {
	Bucket       string
	ObjectPrefix string
}

func newUnloadOperation(client *Client, config *AWSConfiguration, table string, schema *TableSchema) *unloadOperation {
	return &unloadOperation{client, config, table, schema}
}

func (op *unloadOperation) execute() (*UnloadResult, error) {
	statement := op.unloadStatement()
	_, err := op.client.execute(statement)
	if err != nil {
		return nil, err
	}

	result := &UnloadResult{op.config.S3.Bucket, op.table}

	return result, nil
}

func (op *unloadOperation) unloadStatement() string {
	return fmt.Sprintf("UNLOAD ('%s') TO '%s' WITH CREDENTIALS '%s' %s", op.query(), op.staging(), op.credentials(), op.options())
}

func (op *unloadOperation) options() string {
	return fmt.Sprintf("ALLOWOVERWRITE GZIP ESCAPE DELIMITER AS '%s'", op.delimiter())
}

func DefaultDelimiter() string {
	return "|"
}

func (op *unloadOperation) delimiter() string {
	return DefaultDelimiter()
}

func (op *unloadOperation) query() string {
	var columns bytes.Buffer
	for i := 0; i < len(op.schema.Columns); i++ {
		if i > 0 {
			columns.WriteString(",")
		}
		columns.WriteString(op.schema.Columns[i].Name)
	}
	return fmt.Sprintf("SELECT %s FROM %s", columns.String(), op.table)
}

func (op *unloadOperation) staging() string {
	return fmt.Sprintf("s3://%s/%s/", op.config.S3.Bucket, op.table)
}

func (op *unloadOperation) credentials() string {
	return fmt.Sprintf("aws_access_key_id=%s;aws_secret_access_key=%s", op.config.S3.AccessKey, op.config.S3.SecretKey)
}
