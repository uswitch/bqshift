package main

import (
	"fmt"
	"github.com/uswitch/bqshift/bigquery"
	"github.com/uswitch/bqshift/redshift"
	"gopkg.in/alecthomas/kingpin.v2"
	"log"
	"time"
)

var (
	config               = kingpin.Flag("config", "Configuration file with S3 and Redshift credentials").Required().File()
	accessKey            = kingpin.Flag("access-key", "AWS access key. Defaults to $AWS_ACCESS_KEY_ID").OverrideDefaultFromEnvar("AWS_ACCESS_KEY_ID").Required().String()
	secretKey            = kingpin.Flag("secret-access-key", "AWS secret access key. Defaults to $AWS_SECRET_").OverrideDefaultFromEnvar("AWS_SECRET_ACCESS_KEY").Required().String()
	sessionToken         = kingpin.Flag("session-token", "AWS session token.").String()
	project              = kingpin.Flag("project", "Google Project ID").OverrideDefaultFromEnvar("GCLOUD_PROJECT").Required().String()
	overwrite            = kingpin.Flag("overwrite", "Overwrite BigQuery table").Bool()
	usePartitionedTables = kingpin.Flag("partition", "Create time partitioned BigQuery tables.").Bool()
	dateExpression       = kingpin.Flag("date-expression", "Redshift SQL expression to return row date. e.g. CAST(inserted as DATE)").String()
	dateFilter           = kingpin.Flag("date", "Date (YYYY-MM-DD) of partition to filter and load. e.g. 2016-09-30.").String()
	where                = kingpin.Flag("where", "Redshift WHERE clause. Cannot be used with --date/--date-expression. e.g.: CAST(inserted as DATE) < 2016-09-30").String()
	dataset              = kingpin.Arg("dataset", "Destination BigQuery dataset").Required().String()
	table                = kingpin.Arg("table", "Redshift table name").Required().String()
	destinationTable     = kingpin.Arg("destination-table", "BigQuery table name. Defaults to Redshift table name").String()
)

var versionNumber string
var sha string

func version() string {
	if versionNumber == "" {
		return "DEVELOPMENT"
	}
	return fmt.Sprintf("%s (%s)", versionNumber, sha)
}

func partitionFromArgs() (*redshift.DatePartition, error) {
	if *dateExpression == "" {
		return nil, nil
	}
	if *dateFilter == "" {
		return nil, nil
	}

	t, err := time.Parse("2006-01-02", *dateFilter)
	if err != nil {
		return nil, err
	}

	return redshift.NewDatePartition(*dateExpression, t), nil
}

func destinationTableID() string {
	if *destinationTable == "" {
		return *table
	}
	return *destinationTable
}

func main() {
	kingpin.Version(version())
	kingpin.Parse()

	awsConfig, err := redshift.ParseAWSConfiguration(*config)
	if err != nil {
		log.Fatalln("error parsing redshift configuration:", err.Error())
	}
	awsConfig.S3.AccessKey = *accessKey
	awsConfig.S3.SecretKey = *secretKey
	awsConfig.S3.SessionToken = *sessionToken

	config := &Configuration{
		AWS:               awsConfig,
		OverwriteBigQuery: *overwrite,
		DayPartition:      *usePartitionedTables,
		WhereClause:       *where,
	}
	shifter, err := NewShifter(config)
	if err != nil {
		log.Fatalln(err.Error())
	}

	bq := bigquery.NewTableReference(*project, *dataset, destinationTableID())
	partition, err := partitionFromArgs()
	if err != nil {
		log.Fatalln(err.Error())
	}

	if partition != nil {
		bq.DayPartition = &partition.DateFilter
	}

	err = shifter.Run(*table, partition, bq)
	if err != nil {
		log.Fatalln(err.Error())
	}

	log.Println("finished")
}
