package main

import (
	"github.com/uswitch/bqshift/redshift"
)

type Configuration struct {
	OverwriteBigQuery bool
	DayPartition      bool
	AWS               *redshift.AWSConfiguration
}
