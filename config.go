package main

import (
	"github.com/uswitch/bqshift/redshift"
)

type Configuration struct {
	OverwriteBigQuery        bool
	CredentialsConfiguration *redshift.AWSConfiguration
	Verbose                  bool
}
