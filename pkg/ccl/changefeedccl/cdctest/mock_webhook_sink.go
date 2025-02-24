// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cdctest

import (
	"compress/gzip"
	"crypto/tls"
	"io"
	"net/http"
	"net/http/httptest"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/klauspost/compress/zstd"
)

// MockWebhookSink is the Webhook sink used in tests.
type MockWebhookSink struct {
	basicAuth          bool
	username, password string
	server             *httptest.Server
	mu                 struct {
		syncutil.Mutex
		numCalls         int
		responseBodies   map[int][]byte
		statusCodes      []int
		statusCodesIndex int
		rows             []string
		lastHeaders      http.Header
		notify           chan struct{}
	}
}

// StartMockWebhookSinkInsecure starts a mock webhook sink without TLS.
func StartMockWebhookSinkInsecure() (*MockWebhookSink, error) {
	s := makeMockWebhookSink()
	s.server.Start()
	return s, nil
}

// LastRequestHeaders returns the headers from the most recent request.
func (s *MockWebhookSink) LastRequestHeaders() http.Header {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.lastHeaders
}

// StartMockWebhookSink creates and starts a mock webhook sink for tests.
func StartMockWebhookSink(certificate *tls.Certificate) (*MockWebhookSink, error) {
	s := makeMockWebhookSink()
	if certificate == nil {
		return nil, errors.Errorf("Must pass a CA cert when creating a mock webhook sink.")
	}
	s.server.TLS = &tls.Config{
		Certificates: []tls.Certificate{*certificate},
	}
	s.server.StartTLS()
	return s, nil
}

// StartMockWebhookSinkSecure creates and starts a mock webhook sink server that
// requires clients to provide client certificates for authentication.
func StartMockWebhookSinkSecure(certificate *tls.Certificate) (*MockWebhookSink, error) {
	s := makeMockWebhookSink()
	if certificate == nil {
		return nil, errors.Errorf("Must pass a CA cert when creating a mock webhook sink.")
	}

	s.server.TLS = &tls.Config{
		Certificates: []tls.Certificate{*certificate},
		ClientAuth:   tls.RequireAnyClientCert,
	}

	s.server.StartTLS()
	return s, nil
}

// StartMockWebhookSinkWithBasicAuth creates and starts a mock webhook sink for
// tests with basic username/password auth.
func StartMockWebhookSinkWithBasicAuth(
	certificate *tls.Certificate, username, password string,
) (*MockWebhookSink, error) {
	s := makeMockWebhookSink()
	s.basicAuth = true
	s.username = username
	s.password = password
	if certificate != nil {
		s.server.TLS = &tls.Config{
			Certificates: []tls.Certificate{*certificate},
		}
	}
	s.server.StartTLS()
	return s, nil
}

func makeMockWebhookSink() *MockWebhookSink {
	s := &MockWebhookSink{}
	s.mu.statusCodes = []int{http.StatusOK}
	s.mu.responseBodies = make(map[int][]byte)
	s.server = httptest.NewUnstartedServer(http.HandlerFunc(s.requestHandler))
	return s
}

// URL returns the http address of this mock Webhook sink.
func (s *MockWebhookSink) URL() string {
	return s.server.URL
}

// GetNumCalls returns how many times the sink handler has been invoked.
func (s *MockWebhookSink) GetNumCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.numCalls
}

// SetStatusCodes sets the list of HTTP status codes (in order) to use when
// responding to a request (wraps around after completion). Useful for testing
// error handling behavior on client side.
func (s *MockWebhookSink) SetStatusCodes(statusCodes []int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.statusCodes = statusCodes
	s.mu.statusCodesIndex = 0
}

// SetResponse sets the response body and status code to use when responding to
// a request. Useful for testing error handling behavior on client side.
func (s *MockWebhookSink) SetResponse(statusCode int, responseBody []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	numOfStatusCodes := len(s.mu.statusCodes)
	s.mu.statusCodes = append(s.mu.statusCodes, statusCode)
	s.mu.responseBodies[numOfStatusCodes] = responseBody
}

// ClearResponses resets status codes to empty list,
// removes predefined response bodies and resets the index.
func (s *MockWebhookSink) ClearResponses() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.statusCodes = []int{}
	s.mu.responseBodies = map[int][]byte{}
	s.mu.statusCodesIndex = 0
}

// Close closes the mock Webhook sink.
func (s *MockWebhookSink) Close() {
	s.server.Close()
	s.server.CloseClientConnections()
}

// Latest returns the most recent message received by the MockWebhookSink.
func (s *MockWebhookSink) Latest() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.mu.rows) == 0 {
		return ""
	}
	latest := s.mu.rows[len(s.mu.rows)-1]
	return latest
}

// Pop deletes and returns the oldest message from MockWebhookSink.
func (s *MockWebhookSink) Pop() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.mu.rows) > 0 {
		oldest := s.mu.rows[0]
		s.mu.rows = s.mu.rows[1:]
		return oldest
	}
	return ""
}

// NotifyMessage arranges for channel to be closed when message arrives.
func (s *MockWebhookSink) NotifyMessage() chan struct{} {
	c := make(chan struct{})
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.mu.rows) > 0 {
		close(c)
	} else {
		s.mu.notify = c
	}
	return c
}

func (s *MockWebhookSink) requestHandler(hw http.ResponseWriter, hr *http.Request) {
	method := hr.Method

	var err error
	switch {
	case method == http.MethodPost:
		if s.basicAuth {
			username, password, ok := hr.BasicAuth()
			if !ok || s.username != username || s.password != password {
				hw.WriteHeader(http.StatusUnauthorized)
				return
			}
		}
		err = s.publish(hw, hr)
	default:
		hw.WriteHeader(http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(hw, err.Error(), http.StatusInternalServerError)
	}
}

func (s *MockWebhookSink) publish(hw http.ResponseWriter, hr *http.Request) error {
	defer hr.Body.Close()

	s.mu.Lock()
	s.mu.lastHeaders = hr.Header.Clone()
	s.mu.numCalls++
	statusCode := s.mu.statusCodes[s.mu.statusCodesIndex]
	resBody, hasResBody := s.mu.responseBodies[s.mu.statusCodesIndex]
	s.mu.statusCodesIndex = (s.mu.statusCodesIndex + 1) % len(s.mu.statusCodes)
	s.mu.Unlock()

	compression := hr.Header.Get("Content-Encoding")

	if statusCode < http.StatusOK || statusCode >= http.StatusMultipleChoices {
		if !hasResBody {
			hw.WriteHeader(statusCode)
			return nil
		}

		switch compression {
		case "gzip":
			hw.Header().Set("Content-Encoding", "gzip")
			gw := gzip.NewWriter(hw)
			hw.WriteHeader(statusCode)
			if _, err := gw.Write(resBody); err != nil {
				return errors.Wrap(err, "failed to write response body")
			}
			if err := gw.Close(); err != nil {
				return errors.Wrap(err, "failed to flush gzip writer")
			}
			return nil
		case "zstd":
			hw.Header().Set("Content-Encoding", "zstd")
			zw, err := zstd.NewWriter(hw)
			if err != nil {
				return errors.Wrap(err, "failed to create zstd encoder")
			}
			hw.WriteHeader(statusCode)
			if _, err := zw.Write(resBody); err != nil {
				return errors.Wrap(err, "failed to write response body")
			}
			if err := zw.Close(); err != nil {
				return errors.Wrap(err, "failed to flush zstd writer")
			}
			return nil
		}

		hw.WriteHeader(statusCode)
		if _, err := hw.Write(resBody); err != nil {
			return errors.Wrap(err, "failed to write response body")
		}
		return nil
	}

	var row []byte
	switch compression {
	case "gzip":
		gzReader, err := gzip.NewReader(hr.Body)
		if err != nil {
			return errors.Wrap(err, "failed to create gzip reader")
		}
		row, err = io.ReadAll(gzReader)
		if err != nil {
			return errors.Wrap(err, "failed to read compressed request body")
		}
		if err = gzReader.Close(); err != nil {
			return errors.Wrap(err, "failed to close gzip reader")
		}
	case "zstd":
		zstdReader, err := zstd.NewReader(hr.Body)
		if err != nil {
			return errors.Wrap(err, "failed to create zstd reader")
		}
		row, err = io.ReadAll(zstdReader)
		if err != nil {
			return errors.Wrap(err, "failed to read compressed request body")
		}
		zstdReader.Close()
	default:
		var err error
		row, err = io.ReadAll(hr.Body)
		if err != nil {
			return errors.Wrap(err, "failed to read plain request body")
		}
	}

	s.mu.Lock()
	s.mu.rows = append(s.mu.rows, string(row))
	if s.mu.notify != nil {
		close(s.mu.notify)
		s.mu.notify = nil
	}
	s.mu.Unlock()

	hw.WriteHeader(statusCode)
	if hasResBody {
		if _, err := hw.Write(resBody); err != nil {
			return errors.Wrap(err, "failed to write response body")
		}
	}

	return nil
}
