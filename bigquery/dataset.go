package bigquery

import (
	"github.com/uswitch/bqshift/util"
	bigquery "google.golang.org/api/bigquery/v2"
)

func (c *Client) DatasetExists(projectId, datasetName string) (bool, error) {
	resp, err := util.RetryOp(func() (interface{}, error) {
		return c.service.Datasets.List(projectId).All(false).Do()
	})

	if err != nil {
		return false, err
	}

	list := resp.(*bigquery.DatasetList)

	for _, dataset := range list.Datasets {
		if dataset.DatasetReference.DatasetId == datasetName {
			return true, nil
		}
	}

	return false, nil
}
