package redshift

import (
	"bytes"
	"fmt"
)

type unloadOperation struct {
	client *Client
	config *AWSConfiguration
	source *RedshiftSource
}

type UnloadResult struct {
	Bucket       string
	ObjectPrefix string
}

func newUnloadOperation(client *Client, config *AWSConfiguration, source *RedshiftSource) *unloadOperation {
	return &unloadOperation{client, config, source}
}

func (op *unloadOperation) execute() (*UnloadResult, error) {
	statement := op.unloadStatement()
	_, err := op.client.execute(statement)
	if err != nil {
		return nil, err
	}

	result := &UnloadResult{op.config.S3.Bucket, op.source.Table}

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
	for i := 0; i < len(op.source.Schema.Columns); i++ {
		if i > 0 {
			columns.WriteString(",")
		}
		columns.WriteString(op.source.Schema.Columns[i].Name)
	}
	return fmt.Sprintf("SELECT %s FROM %s", columns.String(), op.source.Table)
}

func (op *unloadOperation) staging() string {
	return fmt.Sprintf("s3://%s/%s/", op.config.S3.Bucket, op.source.Table)
}

func (op *unloadOperation) credentials() string {
	return op.config.S3.ToRedshiftCredentialsClause()
}
