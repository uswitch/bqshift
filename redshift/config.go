package redshift

import (
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

type RedshiftConnectionDetails struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Database string `yaml:"db"`
	Password string `yaml:"password"`
}

type S3Configuration struct {
	Bucket      string `yaml:"bucket"`
	Credentials *AWSCredentials
}

type AWSCredentials struct {
	AccessKey string
	SecretKey string
}

func (c *RedshiftConnectionDetails) URLString() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s", c.User, c.Password, c.Host, c.Port, c.Database)
}
