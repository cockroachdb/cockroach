// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cdctest

import (
	"crypto/tls"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// MockHTTPSink is the HTTP sink used in tests.
type MockHTTPSink struct {
	delay              time.Duration
	statusCode         int
	basicAuth          bool
	username, password string
	server             *httptest.Server
	rows               []string
	onRequest          func(sink *MockHTTPSink)
	mu                 struct {
		syncutil.Mutex
	}
}

// StartMockHTTPSinkWithTLS creates and starts a mock HTTP sink for tests with
// TLS enabled.
func StartMockHTTPSinkWithTLS(certificate *tls.Certificate) (*MockHTTPSink, error) {
	s := makeMockHTTPSink()
	if certificate != nil {
		s.server.TLS = &tls.Config{
			Certificates: []tls.Certificate{*certificate},
		}
	}
	s.server.StartTLS()
	return s, nil
}

func makeMockHTTPSink() *MockHTTPSink {
	s := &MockHTTPSink{}
	s.statusCode = http.StatusOK
	s.server = httptest.NewUnstartedServer(http.HandlerFunc(s.requestHandler))
	return s
}

// URL returns the http address of this mock HTTP sink.
func (s *MockHTTPSink) URL() string {
	return s.server.URL
}

// SetOnRequestFunc Injects custom logic into the sink to execute
// before the request gets processed.
func (s *MockHTTPSink) SetOnRequestFunc(fn func(sink *MockHTTPSink)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onRequest = fn
}

// SetDelay sets the amount of time the server should wait before processing a
// webhook request (useful for testing timeout behavior)
func (s *MockHTTPSink) SetDelay(seconds int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.delay = time.Duration(seconds) * time.Second
}

// GetDelay gets the current delay time for requests to be processed in seconds
func (s *MockHTTPSink) GetDelay() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int(s.delay / time.Second)
}

// SetStatusCode sets the HTTP status code to use when responding to a request.
// Useful for testing error handling behavior on client side.
func (s *MockHTTPSink) SetStatusCode(statusCode int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statusCode = statusCode
}

// EnableBasicAuth sets up requirement for server username + password
// authentication.
func (s *MockHTTPSink) EnableBasicAuth(username string, password string) {
	s.basicAuth = true
	s.username = username
	s.password = password
}

// Close closes the mock HTTP sink.
func (s *MockHTTPSink) Close() {
	s.server.Close()
	s.server.CloseClientConnections()
}

// Latest returns the most recent message received by the MockHTTPSink.
func (s *MockHTTPSink) Latest() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.rows) == 0 {
		return ""
	}
	latest := s.rows[len(s.rows)-1]
	return latest
}

// All returns all messages received by the MockHTTPSink
func (s *MockHTTPSink) All() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rows
}

// RemoveFirst deletes the oldest message from MockHTTPSink
func (s *MockHTTPSink) RemoveFirst() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.rows) > 0 {
		s.rows = s.rows[1:]
	}
}

func (s *MockHTTPSink) requestHandler(hw http.ResponseWriter, hr *http.Request) {
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

func (s *MockHTTPSink) publish(hw http.ResponseWriter, hr *http.Request) error {
	if s.onRequest != nil {
		s.onRequest(s)
	}
	time.Sleep(s.delay)
	row, err := ioutil.ReadAll(hr.Body)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.rows = append(s.rows, string(row))
	s.mu.Unlock()
	hw.WriteHeader(s.statusCode)
	defer hr.Body.Close()
	return nil
}
