package main

import (
	"fmt"
	"github.com/uswitch/bqshift/bigquery"
	"github.com/uswitch/bqshift/redshift"
	"gopkg.in/alecthomas/kingpin.v2"
	"os"
)

var (
	config    = kingpin.Flag("config", "configuration file").Required().File()
	accessKey = kingpin.Flag("awsAccessKeyId", "AWS access key").OverrideDefaultFromEnvar("AWS_ACCESS_KEY_ID").Required().String()
	secretKey = kingpin.Flag("secretAccessKey", "AWS secret access key").OverrideDefaultFromEnvar("AWS_SECRET_ACCESS_KEY").Required().String()

	project = kingpin.Flag("project", "google project").OverrideDefaultFromEnvar("GCLOUD_PROJECT").Required().String()
	dataset = kingpin.Arg("dataset", "destination bigquery dataset").Required().String()
	table   = kingpin.Arg("table", "name of table").Required().String()
)

func main() {
	kingpin.Parse()

	config, err := ParseConfiguration(*config)
	if err != nil {
		fmt.Println("error parsing redshift configuration:", err.Error())
		os.Exit(1)
	}
	config.S3.Credentials = &redshift.AWSCredentials{*accessKey, *secretKey}

	shifter, err := NewShifter(config)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	bq := bigquery.NewConfiguration(*project, *dataset, *table)
	err = shifter.Run(*table, bq)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	fmt.Println("finished")
}
