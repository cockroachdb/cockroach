// +build !go1.13

// This file provides glue to use Context in `http.Request` with
// Go version before 1.13.

// The function `http.NewRequestWithContext` has been added in Go 1.13.
// Before the release 1.13, to use Context we need creat `http.Request`
// then use the method `WithContext` to create a new `http.Request`
// with Context from the existing `http.Request`.
//
// Doc: https://golang.org/doc/go1.13#net/http

package jira

import (
	"context"
	"io"
	"net/http"
)

func newRequestWithContext(ctx context.Context, method, url string, body io.Reader) (*http.Request, error) {
	r, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}

	return r.WithContext(ctx), nil
}
