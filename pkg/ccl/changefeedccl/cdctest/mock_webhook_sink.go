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

// MockWebhookSink is the Webhook sink used in tests.
type MockWebhookSink struct {
	delay              time.Duration
	statusCode         int
	basicAuth          bool
	username, password string
	server             *httptest.Server
	rows               []string
	onRequest          func(sink *MockWebhookSink)
	mu                 struct {
		syncutil.Mutex
	}
}

// StartMockWebhookSink creates and starts a mock webhook sink for tests.
func StartMockWebhookSink(certificate *tls.Certificate) (*MockWebhookSink, error) {
	s := makeMockWebhookSink()
	if certificate != nil {
		s.server.TLS = &tls.Config{
			Certificates: []tls.Certificate{*certificate},
		}
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
	s.statusCode = http.StatusOK
	s.server = httptest.NewUnstartedServer(http.HandlerFunc(s.requestHandler))
	return s
}

// URL returns the http address of this mock Webhook sink.
func (s *MockWebhookSink) URL() string {
	return s.server.URL
}

// SetOnRequestFunc Injects custom logic into the sink to execute
// before the request gets processed.
func (s *MockWebhookSink) SetOnRequestFunc(fn func(sink *MockWebhookSink)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onRequest = fn
}

// SetDelay sets the amount of time the server should wait before processing a
// webhook request (useful for testing timeout behavior)
func (s *MockWebhookSink) SetDelay(seconds int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.delay = time.Duration(seconds) * time.Second
}

// GetDelay gets the current delay time for requests to be processed in seconds
func (s *MockWebhookSink) GetDelay() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int(s.delay / time.Second)
}

// SetStatusCode sets the HTTP status code to use when responding to a request.
// Useful for testing error handling behavior on client side.
func (s *MockWebhookSink) SetStatusCode(statusCode int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statusCode = statusCode
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
	if len(s.rows) == 0 {
		return ""
	}
	latest := s.rows[len(s.rows)-1]
	return latest
}

// MessageCount returns the number of messages currently held by MockWebhookSink
func (s *MockWebhookSink) MessageCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.rows)
}

// Pop deletes and returns the oldest message from MockWebhookSink
func (s *MockWebhookSink) Pop() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.rows) > 0 {
		oldest := s.rows[0]
		s.rows = s.rows[1:]
		return oldest
	}
	return ""
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
	return nil
}
