// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package httputil

import (
	"context"
	"crypto/tls"
	"crypto/x509"
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

type clientOptions struct {
	clientTimeout time.Duration
	dialerTimeout time.Duration
	customCAPEM   string
}

// ClientOption overrides behavior of NewClient.
type ClientOption interface {
	apply(*clientOptions)
}

type clientOptionFunc func(options *clientOptions)

func (f clientOptionFunc) apply(o *clientOptions) {
	f(o)
}

// WithClientTimeout sets the client timeout for the http.Client.
func WithClientTimeout(timeout time.Duration) ClientOption {
	return clientOptionFunc(func(o *clientOptions) {
		o.clientTimeout = timeout
	})
}

// WithDialerTimeout sets the dialer timeout for the http.Client.
func WithDialerTimeout(timeout time.Duration) ClientOption {
	return clientOptionFunc(func(o *clientOptions) {
		o.dialerTimeout = timeout
	})
}

// WithCustomCAPEM sets the custom root CA for the http.Client.
func WithCustomCAPEM(customCAPEM string) ClientOption {
	return clientOptionFunc(func(o *clientOptions) {
		o.customCAPEM = customCAPEM
	})
}

// NewClient defines a new http.Client as per the provided options.
func NewClient(opts ...ClientOption) *Client {
	options := &clientOptions{}
	for _, o := range opts {
		o.apply(options)
	}

	t := http.DefaultTransport.(*http.Transport)
	var tlsConf *tls.Config
	if options.customCAPEM != "" {
		certPool := x509.NewCertPool()
		if !certPool.AppendCertsFromPEM([]byte(options.customCAPEM)) {
			return nil
		}
		tlsConf = &tls.Config{
			RootCAs: certPool,
		}
	}

	return &Client{
		&http.Client{
			Timeout: options.clientTimeout,
			Transport: &http.Transport{
				// Don't leak a goroutine on OSX (the TCP level timeout is probably
				// much higher than on linux).
				DialContext:           (&net.Dialer{Timeout: options.dialerTimeout}).DialContext,
				DisableKeepAlives:     true,
				Proxy:                 t.Proxy,
				MaxIdleConns:          t.MaxIdleConns,
				IdleConnTimeout:       t.IdleConnTimeout,
				TLSHandshakeTimeout:   t.TLSHandshakeTimeout,
				ExpectContinueTimeout: t.ExpectContinueTimeout,
				TLSClientConfig:       tlsConf,
			},
		},
	}
}

// NewClientWithTimeout defines a http.Client with the given timeout.
// TODO(pritesh-lahoti): Deprecate this in favor of NewClient.
func NewClientWithTimeout(timeout time.Duration) *Client {
	return NewClient(
		WithClientTimeout(timeout),
		WithDialerTimeout(timeout),
	)
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

// Put is like http.Put but uses the provided context and obeys its cancellation.
// It also uses the default client with a default 3 second timeout.
func Put(
	ctx context.Context, url string, h *http.Header, body io.Reader,
) (resp *http.Response, err error) {
	return DefaultClient.Put(ctx, url, h, body)
}

// Delete is like http.Delete but uses the provided context and obeys its cancellation.
// It also uses the default client with a default 3 second timeout.
func Delete(ctx context.Context, url string, h *http.Header) (resp *http.Response, err error) {
	return DefaultClient.Delete(ctx, url, h)
}

// Get does like http.Client.Get but uses the provided context and obeys its cancellation.
func (c *Client) Get(ctx context.Context, url string) (resp *http.Response, err error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	return c.Do(req)
}

// Put is like http.Client.Put but uses the provided context and obeys its cancellation.
// RequestHeaders can be used to set the following http headers:
// 1. ContentType
// 2. Authorization
func (c *Client) Put(
	ctx context.Context, url string, h *http.Header, body io.Reader,
) (resp *http.Response, err error) {
	req, err := http.NewRequestWithContext(ctx, "PUT", url, body)
	if err != nil {
		return nil, err
	}
	if h != nil {
		req.Header = *h
	}
	return c.Do(req)
}

// Delete is like http.Client.Delete but uses the provided context and obeys its cancellation.
func (c *Client) Delete(
	ctx context.Context, url string, h *http.Header,
) (resp *http.Response, err error) {
	req, err := http.NewRequestWithContext(ctx, "DELETE", url, nil)
	if err != nil {
		return nil, err
	}
	if h != nil {
		req.Header = *h
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
