package redshift

import (
	"fmt"
)

type RedshiftConfiguration struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Database string `yaml:"db"`
	Password string `yaml:"password"`
}

type AWSCredentials struct {
	AccessKey string
	SecretKey string
}

type S3Configuration struct {
	Bucket      string `yaml:"bucket"`
	Credentials *AWSCredentials
}

func (c *RedshiftConfiguration) URLString() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s", c.User, c.Password, c.Host, c.Port, c.Database)
}
