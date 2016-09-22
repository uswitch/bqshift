package main

import (
	"github.com/uswitch/bqshift/bigquery"
	"github.com/uswitch/bqshift/redshift"
	"gopkg.in/alecthomas/kingpin.v2"
	"log"
)

var (
	config    = kingpin.Flag("config", "configuration file").Required().File()
	accessKey = kingpin.Flag("awsAccessKeyId", "AWS access key").OverrideDefaultFromEnvar("AWS_ACCESS_KEY_ID").Required().String()
	secretKey = kingpin.Flag("secretAccessKey", "AWS secret access key").OverrideDefaultFromEnvar("AWS_SECRET_ACCESS_KEY").Required().String()
	project   = kingpin.Flag("project", "google project").OverrideDefaultFromEnvar("GCLOUD_PROJECT").Required().String()
	overwrite = kingpin.Flag("overwrite", "overwrite bigquery table").Bool()
	dataset   = kingpin.Arg("dataset", "destination bigquery dataset").Required().String()
	table     = kingpin.Arg("table", "name of table").Required().String()
)

func main() {
	kingpin.Parse()

	credentials, err := ParseCredentialsConfiguration(*config)
	if err != nil {
		log.Fatalln("error parsing redshift configuration:", err.Error())
	}
	credentials.S3.Credentials = &redshift.AWSCredentials{*accessKey, *secretKey}
	config := &Configuration{
		CredentialsConfiguration: credentials,
		OverwriteBigQuery:        *overwrite,
	}

	shifter, err := NewShifter(config)
	if err != nil {
		log.Fatalln(err.Error())
	}

	bq := bigquery.NewConfiguration(*project, *dataset, *table)
	err = shifter.Run(*table, bq)
	if err != nil {
		log.Fatalln(err.Error())
	}

	log.Println("finished")
}
