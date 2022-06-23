package circuit

import (
	"io"
	"net/http"
	"net/url"
	"time"
)

// HTTPClient is a wrapper around http.Client that provides circuit breaker capabilities.
//
// By default, the client will use its defaultBreaker. A BreakerLookup function may be
// provided to allow different breakers to be used based on the circumstance. See the
// implementation of NewHostBasedHTTPClient for an example of this.
type HTTPClient struct {
	Client         *http.Client
	BreakerTripped func()
	BreakerReset   func()
	BreakerLookup  func(*HTTPClient, interface{}) *Breaker
	Panel          *Panel
	timeout        time.Duration
}

var defaultBreakerName = "_default"

// NewHTTPClient provides a circuit breaker wrapper around http.Client.
// It wraps all of the regular http.Client functions. Specifying 0 for timeout will
// give a breaker that does not check for time outs.
func NewHTTPClient(timeout time.Duration, threshold int64, client *http.Client) *HTTPClient {
	breaker := NewThresholdBreaker(threshold)
	return NewHTTPClientWithBreaker(breaker, timeout, client)
}

// NewHostBasedHTTPClient provides a circuit breaker wrapper around http.Client. This
// client will use one circuit breaker per host parsed from the request URL. This allows
// you to use a single HTTPClient for multiple hosts with one host's breaker not affecting
// the other hosts.
func NewHostBasedHTTPClient(timeout time.Duration, threshold int64, client *http.Client) *HTTPClient {
	brclient := NewHTTPClient(timeout, threshold, client)

	brclient.BreakerLookup = func(c *HTTPClient, val interface{}) *Breaker {
		rawURL := val.(string)
		parsedURL, err := url.Parse(rawURL)
		if err != nil {
			breaker, _ := c.Panel.Get(defaultBreakerName)
			return breaker
		}
		host := parsedURL.Host

		cb, ok := c.Panel.Get(host)
		if !ok {
			cb = NewThresholdBreaker(threshold)
			c.Panel.Add(host, cb)
		}

		return cb
	}

	return brclient
}

// NewHTTPClientWithBreaker provides a circuit breaker wrapper around http.Client.
// It wraps all of the regular http.Client functions using the provided Breaker.
func NewHTTPClientWithBreaker(breaker *Breaker, timeout time.Duration, client *http.Client) *HTTPClient {
	if client == nil {
		client = &http.Client{}
	}

	panel := NewPanel()
	panel.Add(defaultBreakerName, breaker)

	brclient := &HTTPClient{Client: client, Panel: panel, timeout: timeout}
	brclient.BreakerLookup = func(c *HTTPClient, val interface{}) *Breaker {
		cb, _ := c.Panel.Get(defaultBreakerName)
		return cb
	}

	events := breaker.Subscribe()
	go func() {
		event := <-events
		switch event {
		case BreakerTripped:
			brclient.runBreakerTripped()
		case BreakerReset:
			brclient.runBreakerReset()
		}
	}()

	return brclient
}

// Do wraps http.Client Do()
func (c *HTTPClient) Do(req *http.Request) (*http.Response, error) {
	var resp *http.Response
	var err error
	breaker := c.breakerLookup(req.URL.String())
	err = breaker.Call(func() error {
		resp, err = c.Client.Do(req)
		return err
	}, c.timeout)
	return resp, err
}

// Get wraps http.Client Get()
func (c *HTTPClient) Get(url string) (*http.Response, error) {
	var resp *http.Response
	breaker := c.breakerLookup(url)
	err := breaker.Call(func() error {
		aresp, err := c.Client.Get(url)
		resp = aresp
		return err
	}, c.timeout)
	return resp, err
}

// Head wraps http.Client Head()
func (c *HTTPClient) Head(url string) (*http.Response, error) {
	var resp *http.Response
	breaker := c.breakerLookup(url)
	err := breaker.Call(func() error {
		aresp, err := c.Client.Head(url)
		resp = aresp
		return err
	}, c.timeout)
	return resp, err
}

// Post wraps http.Client Post()
func (c *HTTPClient) Post(url string, bodyType string, body io.Reader) (*http.Response, error) {
	var resp *http.Response
	breaker := c.breakerLookup(url)
	err := breaker.Call(func() error {
		aresp, err := c.Client.Post(url, bodyType, body)
		resp = aresp
		return err
	}, c.timeout)
	return resp, err
}

// PostForm wraps http.Client PostForm()
func (c *HTTPClient) PostForm(url string, data url.Values) (*http.Response, error) {
	var resp *http.Response
	breaker := c.breakerLookup(url)
	err := breaker.Call(func() error {
		aresp, err := c.Client.PostForm(url, data)
		resp = aresp
		return err
	}, c.timeout)
	return resp, err
}

func (c *HTTPClient) breakerLookup(val interface{}) *Breaker {
	if c.BreakerLookup != nil {
		return c.BreakerLookup(c, val)
	}
	cb, _ := c.Panel.Get(defaultBreakerName)
	return cb
}

func (c *HTTPClient) runBreakerTripped() {
	if c.BreakerTripped != nil {
		c.BreakerTripped()
	}
}

func (c *HTTPClient) runBreakerReset() {
	if c.BreakerReset != nil {
		c.BreakerReset()
	}
}
