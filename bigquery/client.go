package bigquery

import (
	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
	bq "google.golang.org/api/bigquery/v2"
)

type Client struct {
	service *bq.Service
}

func NewClient() (*Client, error) {
	ctx := context.Background()
	client, err := google.DefaultClient(ctx, bq.BigqueryScope)
	svc, err := bq.New(client)
	if err != nil {
		return nil, err
	}

	return &Client{svc}, nil
}
