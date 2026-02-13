// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"maps"
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

	if *c.UnsafeTLS {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}

	hs := &httpSink{
		client: http.Client{
			Transport: transport,
			Timeout:   *c.Timeout,
		},
		config: &c,
	}

	method := string(*c.Method)

	// request we'll reuse by cloning in the output method
	request, err := http.NewRequest(method, *c.Address, http.NoBody)
	if err != nil {
		return nil, err
	}

	if method == http.MethodPost {
		contentType := "application/octet-stream"
		fConstructor, ok := formatters[*c.Format]
		if !ok {
			panic(errors.AssertionFailedf("unknown format: %q", *c.Format))
		}
		f := fConstructor()
		if f.contentType() != "" {
			contentType = f.contentType()
		}
		request.Header.Set(httputil.ContentTypeHeader, contentType)

		if *c.Compression == logconfig.GzipCompression {
			hs.gzipWriter = gzip.NewWriter(io.Discard)
			// add gzip header if the method is POST and compression is gzip
			request.Header.Set(httputil.ContentEncodingHeader, httputil.GzipEncoding)
		}
	}

	for key, val := range c.Headers {
		request.Header.Set(key, val)
	}

	dhFilepaths := make(map[string]string, len(c.FileBasedHeaders))
	maps.Copy(dhFilepaths, c.FileBasedHeaders)
	if len(dhFilepaths) > 0 {
		hs.dynamicHeaders = &dynamicHeaders{
			headerToFilepath: dhFilepaths,
		}
		err := hs.RefreshDynamicHeaders()
		if err != nil {
			return nil, err
		}
		hs.dynamicHeaders.mu.Lock()
		defer hs.dynamicHeaders.mu.Unlock()
		for k, v := range hs.dynamicHeaders.mu.headerToValue {
			request.Header.Add(k, v)
		}
	}

	hs.request = request

	return hs, nil
}

type httpSink struct {
	client     http.Client
	config     *logconfig.HTTPSinkConfig
	request    *http.Request
	gzipWriter *gzip.Writer
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
	req := hs.request.Clone(context.Background())

	if *hs.config.Method == http.MethodPost {
		// request body should contain the log message
		var buf = bytes.Buffer{}

		if *hs.config.Compression == logconfig.GzipCompression {
			hs.gzipWriter.Reset(&buf)
			_, err := hs.gzipWriter.Write(b)
			if err != nil {
				return err
			}
			err = hs.gzipWriter.Close()
			if err != nil {
				return err
			}
		} else {
			buf.Write(b)
		}

		req.Body = io.NopCloser(bytes.NewReader(buf.Bytes()))
	} else {
		req.URL.RawQuery = url.QueryEscape(string(b))
	}

	resp, err := hs.client.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()

	if resp.StatusCode >= 400 {
		return HTTPLogError{
			StatusCode: resp.StatusCode,
			Address:    *hs.config.Address,
		}
	}
	return nil
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
