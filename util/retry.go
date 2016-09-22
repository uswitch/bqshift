package util

import (
	"github.com/cenk/backoff"
)

func RetryOp(op func() (interface{}, error)) (interface{}, error) {
	resultch := make(chan interface{}, 1)

	retryOp := func() error {
		result, err := op()
		if err != nil {
			return err
		}
		resultch <- result
		return nil
	}

	err := backoff.Retry(retryOp, backoff.NewExponentialBackOff())

	if err != nil {
		return nil, err
	}

	return <-resultch, err
}
