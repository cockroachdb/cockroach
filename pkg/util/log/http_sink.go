// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
		address:     *c.Address,
		doRequest:   doPost,
		contentType: "application/octet-stream",
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
	dhFilepaths := make(map[string]string, len(c.Headers))
	for key, val := range c.Headers {
		staticHeaders[key] = val
	}
	for key, val := range c.FileBasedHeaders {
		dhFilepaths[key] = val
	}
	hs.staticHeaders = staticHeaders
	if len(dhFilepaths) > 0 {
		hs.dynamicHeaders = &dynamicHeaders{
			headerToFilepath: dhFilepaths,
		}
		err := hs.RefreshDynamicHeaders()
		if err != nil {
			return nil, err
		}
	}
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
	// It will be nil if there are no filepaths provided.
	dynamicHeaders *dynamicHeaders
}

type dynamicHeaders struct {
	headerToFilepath map[string]string
	mu               struct {
		syncutil.Mutex
		headerToValue map[string]string
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
	// If the filepathMap was populated we know to check the values.
	if hs.dynamicHeaders != nil {
		func() {
			hs.dynamicHeaders.mu.Lock()
			defer hs.dynamicHeaders.mu.Unlock()
			for k, v := range hs.dynamicHeaders.mu.headerToValue {
				req.Header.Add(k, v)
			}
		}()
	}
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

// RefreshDynamicHeaders loads and sets the new dynamic headers for a given sink.
// It iterates over dynamicHeaders.filepath reading each file for contents and then
// updating dynamicHeaders.mu.value.
func (hs *httpSink) RefreshDynamicHeaders() error {
	if hs.dynamicHeaders == nil {
		return nil
	}
	dhVals := make(map[string]string, len(hs.dynamicHeaders.headerToFilepath))
	for key, filepath := range hs.dynamicHeaders.headerToFilepath {
		data, err := os.ReadFile(filepath)
		if err != nil {
			return err
		}
		dhVals[key] = string(data)
	}
	hs.dynamicHeaders.mu.Lock()
	defer hs.dynamicHeaders.mu.Unlock()
	hs.dynamicHeaders.mu.headerToValue = dhVals
	return nil
}
