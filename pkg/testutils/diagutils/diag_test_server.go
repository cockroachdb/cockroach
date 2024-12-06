// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package diagutils

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/diagnostics/diagnosticspb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// Server is a http server that implements a diagnostics endpoint. Its URL can
// be used as the updates or reporting URL (see diagonsticspb.TestingKnobs).
type Server struct {
	httpSrv *httptest.Server
	url     *url.URL

	mu struct {
		syncutil.Mutex

		numRequests int
		last        *RequestData

		// Testing knobs. Setting these will override response from the test server.
		respError   error
		respCode    int
		waitSeconds int
	}
}

// RequestData stores the data provided by a diagnostics request.
type RequestData struct {
	UUID          string
	TenantID      string
	NodeID        string
	SQLInstanceID string
	Version       string
	LicenseType   string
	Internal      string
	RawReportBody string

	diagnosticspb.DiagnosticReport
}

// NewServer creates and starts a new server. The server must be closed.
func NewServer() *Server {
	srv := &Server{}

	srv.httpSrv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		body, err := io.ReadAll(r.Body)
		if err != nil {
			panic(err)
		}

		srv.mu.Lock()
		defer srv.mu.Unlock()

		srv.mu.numRequests++

		data := &RequestData{
			UUID:          r.URL.Query().Get("uuid"),
			TenantID:      r.URL.Query().Get("tenantid"),
			NodeID:        r.URL.Query().Get("nodeid"),
			SQLInstanceID: r.URL.Query().Get("sqlid"),
			Version:       r.URL.Query().Get("version"),
			LicenseType:   r.URL.Query().Get("licensetype"),
			Internal:      r.URL.Query().Get("internal"),
			RawReportBody: string(body),
		}

		// TODO(dt): switch on the request path to handle different request types.
		if err := protoutil.Unmarshal(body, &data.DiagnosticReport); err != nil {
			panic(err)
		}
		srv.mu.last = data

		if srv.mu.waitSeconds > 0 {
			time.Sleep(time.Duration(srv.mu.waitSeconds) * time.Second)
		}

		if srv.mu.respError != nil {
			http.Error(w, srv.mu.respError.Error(), srv.mu.respCode)
		} else if srv.mu.respCode != 0 {
			w.WriteHeader(srv.mu.respCode)
		}
	}))

	var err error
	srv.url, err = url.Parse(srv.httpSrv.URL)
	if err != nil {
		panic(err)
	}

	return srv
}

// URL returns the URL that can be used to send requests to the server.
func (s *Server) URL() *url.URL {
	return s.url
}

// Close shuts down the server and blocks until all outstanding
// requests on this server have completed.
func (s *Server) Close() {
	s.httpSrv.Close()
}

// NumRequests returns the total number of requests received by this server.
func (s *Server) NumRequests() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.numRequests
}

// LastRequestData returns the data from last request received by the server.
// Returns nil if there were no requests.
func (s *Server) LastRequestData() *RequestData {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.last
}

func (s *Server) SetRespError(e error) func() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.mu.respError = e
	return func() {
		s.SetRespError(nil)
	}
}

func (s *Server) SetRespCode(code int) func() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.mu.respCode = code
	return func() {
		s.SetRespCode(0)
	}
}

func (s *Server) SetWaitSeconds(seconds int) func() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.mu.waitSeconds = seconds
	return func() {
		s.SetWaitSeconds(0)
	}
}
