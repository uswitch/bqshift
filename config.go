package main

import (
	"github.com/uswitch/bqshift/redshift"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
)

type Configuration struct {
	OverwriteBigQuery        bool
	CredentialsConfiguration *CredentialsConfiguration
	Verbose                  bool
	WaitForLoad              bool
}

type CredentialsConfiguration struct {
	Redshift *redshift.RedshiftConfiguration `yaml:"redshift"`
	S3       *redshift.S3Configuration       `yaml:"s3"`
}

func ParseCredentialsConfiguration(file *os.File) (*CredentialsConfiguration, error) {
	contents, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, err
	}

	var c CredentialsConfiguration
	err = yaml.Unmarshal(contents, &c)
	if err != nil {
		return nil, err
	}

	return &c, nil
}
