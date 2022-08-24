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
	"io"
	"net/http"
	"net/http/httptest"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// MockWebhookSink is the Webhook sink used in tests.
type MockWebhookSink struct {
	basicAuth          bool
	username, password string
	server             *httptest.Server
	mu                 struct {
		syncutil.Mutex
		numCalls         int
		statusCodes      []int
		statusCodesIndex int
		rows             []string
		notify           chan struct{}
	}
}

// StartMockWebhookSinkInsecure starts a mock webhook sink without TLS.
func StartMockWebhookSinkInsecure() (*MockWebhookSink, error) {
	s := makeMockWebhookSink()
	s.server.Start()
	return s, nil
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
// requires clients to provide client certificates for authentication
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

// Pop deletes and returns the oldest message from MockWebhookSink
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
	row, err := io.ReadAll(hr.Body)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.mu.numCalls++
	if s.mu.statusCodes[s.mu.statusCodesIndex] >= http.StatusOK && s.mu.statusCodes[s.mu.statusCodesIndex] < http.StatusMultipleChoices {
		s.mu.rows = append(s.mu.rows, string(row))
		if s.mu.notify != nil {
			close(s.mu.notify)
			s.mu.notify = nil
		}
	}

	hw.WriteHeader(s.mu.statusCodes[s.mu.statusCodesIndex])
	s.mu.statusCodesIndex = (s.mu.statusCodesIndex + 1) % len(s.mu.statusCodes)
	s.mu.Unlock()
	return nil
}
