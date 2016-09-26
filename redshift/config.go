package redshift

import (
	"bytes"
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
)

type AWSConfiguration struct {
	Redshift *RedshiftConnectionDetails `yaml:"redshift"`
	S3       *S3Configuration           `yaml:"s3"`
}

func ParseAWSConfiguration(file *os.File) (*AWSConfiguration, error) {
	contents, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, err
	}

	var c AWSConfiguration
	err = yaml.Unmarshal(contents, &c)
	if err != nil {
		return nil, err
	}

	return &c, nil
}

type DatePartition struct {
	DateExpression string
	DateFilter     string
}

type RedshiftSource struct {
	Table     string
	Schema    *TableSchema
	Partition *DatePartition
}

func (s *RedshiftSource) SelectClause() string {
	var columns bytes.Buffer
	for i := 0; i < len(s.Schema.Columns); i++ {
		if i > 0 {
			columns.WriteString(",")
		}
		columns.WriteString(s.Schema.Columns[i].Name)
	}
	return fmt.Sprintf("SELECT %s FROM %s", columns.String(), s.Table)
}

type RedshiftConnectionDetails struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Database string `yaml:"db"`
	Password string `yaml:"password"`
}

type S3Configuration struct {
	Bucket    string `yaml:"bucket"`
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`
}

func (c S3Configuration) ToRedshiftCredentialsClause() string {
	return fmt.Sprintf("aws_access_key_id=%s;aws_secret_access_key=%s", c.AccessKey, c.SecretKey)
}

func (c *RedshiftConnectionDetails) URLString() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s", c.User, c.Password, c.Host, c.Port, c.Database)
}
