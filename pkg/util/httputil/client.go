// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package httputil

import (
	"context"
	"io"
	"net"
	"net/http"
	"time"
)

// DefaultClient is a replacement for http.DefaultClient which defines
// a standard timeout.
var DefaultClient = NewClientWithTimeout(StandardHTTPTimeout)

// StandardHTTPTimeout is the default timeout to use for HTTP connections.
const StandardHTTPTimeout time.Duration = 3 * time.Second

// NewClientWithTimeout defines a http.Client with the given timeout.
func NewClientWithTimeout(timeout time.Duration) *Client {
	return &Client{&http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			// Don't leak a goroutine on OSX (the TCP level timeout is probably
			// much higher than on linux).
			DialContext:       (&net.Dialer{Timeout: timeout}).DialContext,
			DisableKeepAlives: true,
		},
	}}
}

// Client is a replacement for http.Client which implements method
// variants that respect a provided context's cancellation status.
type Client struct {
	*http.Client
}

// Get does like http.Get but uses the provided context and obeys its cancellation.
// It also uses the default client with a default 3 second timeout.
func Get(ctx context.Context, url string) (resp *http.Response, err error) {
	return DefaultClient.Get(ctx, url)
}

// Head does like http.Head but uses the provided context and obeys its cancellation.
// It also uses the default client with a default 3 second timeout.
func Head(ctx context.Context, url string) (resp *http.Response, err error) {
	return DefaultClient.Head(ctx, url)
}

// Post does like http.Post but uses the provided context and obeys its cancellation.
// It also uses the default client with a default 3 second timeout.
func Post(
	ctx context.Context, url, contentType string, body io.Reader,
) (resp *http.Response, err error) {
	return DefaultClient.Post(ctx, url, contentType, body)
}

// Get does like http.Client.Get but uses the provided context and obeys its cancellation.
func (c *Client) Get(ctx context.Context, url string) (resp *http.Response, err error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	return c.Do(req)
}

// Head does like http.Client.Head but uses the provided context and obeys its cancellation.
func (c *Client) Head(ctx context.Context, url string) (resp *http.Response, err error) {
	req, err := http.NewRequestWithContext(ctx, "HEAD", url, nil)
	if err != nil {
		return nil, err
	}
	return c.Do(req)
}

// Post does like http.Client.Post but uses the provided context and obeys its cancellation.
func (c *Client) Post(
	ctx context.Context, url, contentType string, body io.Reader,
) (resp *http.Response, err error) {
	req, err := http.NewRequestWithContext(ctx, "POST", url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", contentType)
	return c.Do(req)
}
