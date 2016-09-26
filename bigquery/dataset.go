package bigquery

import (
	"github.com/uswitch/bqshift/util"
	bigquery "google.golang.org/api/bigquery/v2"
)

func (c *Client) DatasetExists(ref *DatasetReference) (bool, error) {
	resp, err := util.RetryOp(func() (interface{}, error) {
		return c.service.Datasets.List(ref.ProjectID).All(false).Do()
	})

	if err != nil {
		return false, err
	}

	list := resp.(*bigquery.DatasetList)

	for _, dataset := range list.Datasets {
		if dataset.DatasetReference.DatasetId == ref.DatasetID {
			return true, nil
		}
	}

	return false, nil
}
