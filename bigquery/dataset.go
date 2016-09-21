package bigquery

func (c *Client) DatasetExists(projectId, datasetName string) (bool, error) {
	list, err := c.service.Datasets.List(projectId).All(false).Do()
	if err != nil {
		return false, err
	}

	for _, dataset := range list.Datasets {
		if dataset.DatasetReference.DatasetId == datasetName {
			return true, nil
		}
	}

	return false, nil
}
