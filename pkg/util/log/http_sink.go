// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package log

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"fmt"
	"net/http"
	"net/url"
	"os"

	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// TODO: HTTP requests should be bound to context via http.NewRequestWithContext
// Proper logging context to be decided/designed.
func newHTTPSink(c logconfig.HTTPSinkConfig) (*httpSink, error) {
	transport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return nil, errors.AssertionFailedf("http.DefaultTransport is not a http.Transport: %T", http.DefaultTransport)
	}
	transport = transport.Clone()
	transport.DisableKeepAlives = *c.DisableKeepAlives
	hs := &httpSink{
		client: http.Client{
			Transport: transport,
			Timeout:   *c.Timeout,
		},
		address:        *c.Address,
		doRequest:      doPost,
		contentType:    "application/octet-stream",
		dynamicHeaders: &dynamicHeaders{},
	}

	if *c.UnsafeTLS {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}

	if string(*c.Method) == http.MethodGet {
		hs.doRequest = doGet
	}

	fConstructor, ok := formatters[*c.Format]
	if !ok {
		panic(errors.AssertionFailedf("unknown format: %q", *c.Format))
	}
	f := fConstructor()
	if f.contentType() != "" {
		hs.contentType = f.contentType()
	}

	hs.config = &c

	staticHeaders := make(map[string]string, len(c.Headers))
	for key, hVal := range c.Headers {
		if hVal.Value != "" {
			staticHeaders[key] = hVal.Value
		}
	}
	dynamicHeaders, err := generateDynamicHeaders(c.Headers)
	if err != nil {
		return nil, err
	}
	hs.staticHeaders = staticHeaders
	hs.dynamicHeaders.mu.headers = dynamicHeaders

	return hs, nil
}

type httpSink struct {
	client      http.Client
	address     string
	contentType string
	doRequest   func(sink *httpSink, logEntry []byte) (*http.Response, error)
	config      *logconfig.HTTPSinkConfig
	// staticHeaders holds all the config headers defined by direct values.
	staticHeaders map[string]string
	// dynamicHeaders holds all the config headers defined by values from files.
	dynamicHeaders *dynamicHeaders
}

type dynamicHeaders struct {
	mu struct {
		syncutil.Mutex
		headers map[string]string
	}
}

// output emits some formatted bytes to this sink.
// the sink is invited to perform an extra flush if indicated
// by the argument. This is set to true for e.g. Fatal
// entries.
//
// The parent logger's outputMu is held during this operation: log
// sinks must not recursively call into logging when implementing
// this method.
func (hs *httpSink) output(b []byte, opt sinkOutputOptions) (err error) {
	resp, err := hs.doRequest(hs, b)
	if err != nil {
		return err
	}

	if resp.StatusCode >= 400 {
		return HTTPLogError{
			StatusCode: resp.StatusCode,
			Address:    hs.address,
		}
	}
	return nil
}

func doPost(hs *httpSink, b []byte) (*http.Response, error) {
	var buf = bytes.Buffer{}
	var req *http.Request

	if *hs.config.Compression == logconfig.GzipCompression {
		g := gzip.NewWriter(&buf)
		_, err := g.Write(b)
		if err != nil {
			return nil, err
		}
		err = g.Close()
		if err != nil {
			return nil, err
		}
	} else {
		buf.Write(b)
	}

	req, err := http.NewRequest(http.MethodPost, hs.address, &buf)
	if err != nil {
		return nil, err
	}

	if *hs.config.Compression == logconfig.GzipCompression {
		req.Header.Add(httputil.ContentEncodingHeader, httputil.GzipEncoding)
	}

	// Add both the staticHeaders and dynamicHeaders to the request.
	for k, v := range hs.staticHeaders {
		req.Header.Add(k, v)
	}
	func() {
		hs.dynamicHeaders.mu.Lock()
		defer hs.dynamicHeaders.mu.Unlock()
		for k, v := range hs.dynamicHeaders.mu.headers {
			req.Header.Add(k, v)
		}
	}()
	req.Header.Add(httputil.ContentTypeHeader, hs.contentType)
	resp, err := hs.client.Do(req)
	if err != nil {
		return nil, err
	}
	resp.Body.Close() // don't care about content
	return resp, nil
}

func doGet(hs *httpSink, b []byte) (*http.Response, error) {
	resp, err := hs.client.Get(hs.address + "?" + url.QueryEscape(string(b)))
	if err != nil {
		return nil, err
	}
	resp.Body.Close() // don't care about content
	return resp, nil
}

// active returns true if this sink is currently active.
func (*httpSink) active() bool {
	return true
}

// attachHints attaches some hints about the location of the message
// to the stack message.
func (*httpSink) attachHints(stacks []byte) []byte {
	return stacks
}

// exitCode returns the exit code to use if the logger decides
// to terminate because of an error in output().
func (*httpSink) exitCode() exit.Code {
	return exit.LoggingNetCollectorUnavailable()
}

// HTTPLogError represents an HTTP error status code from a logging request.
type HTTPLogError struct {
	StatusCode int
	Address    string
}

func (e HTTPLogError) Error() string {
	return fmt.Sprintf(
		"received %v response attempting to log to [%v]",
		e.StatusCode, e.Address)
}

// RefreshValuesFromFiles will iterate over all http sinks and replace the sink's
// dynamicHeaders with newly generated dynamicHeaders.
func RefreshValuesFromFiles() {
	_ = logging.allSinkInfos.iterHttpSinks(func(hs *httpSink) error {
		if len(hs.config.Headers) == 0 {
			return nil
		}
		dynamicHeaders, err := generateDynamicHeaders(hs.config.Headers)
		if err != nil {
			return err
		}
		hs.dynamicHeaders.mu.Lock()
		defer hs.dynamicHeaders.mu.Unlock()
		hs.dynamicHeaders.mu.headers = dynamicHeaders
		return nil
	})
}

// generateDynamicHeaders takes a headers map and creates a new string map
// by reading the values that any headers provide by a filepath.
func generateDynamicHeaders(headers map[string]*logconfig.HeaderValue) (map[string]string, error) {
	dynamicHeaders := make(map[string]string, len(headers))
	for key, hVal := range headers {
		if hVal.Filepath != "" {
			data, err := os.ReadFile(hVal.Filepath)
			if err != nil {
				return nil, err
			}
			dynamicHeaders[key] = string(data)
		}
	}
	return dynamicHeaders, nil
}
