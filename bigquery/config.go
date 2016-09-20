package bigquery

type Configuration struct {
	ProjectID   string
	DatasetName string
	TableName   string
}

func NewConfiguration(projectId, dataset, table string) *Configuration {
	return &Configuration{projectId, dataset, table}
}
