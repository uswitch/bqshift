package main

import (
	"github.com/uswitch/bqshift/redshift"
)

type Configuration struct {
	OverwriteBigQuery bool
	AWS               *redshift.AWSConfiguration
	Verbose           bool
}
