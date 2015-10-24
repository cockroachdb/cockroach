// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)
// Author: Bram Gruneir (bram+code@cockroachlabs.com)

package server

import (
	// This is imported for its side-effect of registering expvar
	// endpoints with the http.DefaultServeMux.
	_ "expvar"
	"fmt"
	"net/http"
	"time"

	// Register the net/trace endpoint with http.DefaultServeMux.
	_ "golang.org/x/net/trace"
	// This is imported for its side-effect of registering pprof
	// endpoints with the http.DefaultServeMux.
	_ "net/http/pprof"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/stop"
)

const (
	// adminEndpoint is the prefix for RESTful endpoints used to
	// provide an administrative interface to the cockroach cluster.
	adminEndpoint = "/_admin/"
	// debugEndpoint is the prefix of golang's standard debug functionality
	// for access to exported vars and pprof tools.
	debugEndpoint = "/debug/"
	// healthPath is the health endpoint.
	healthPath = adminEndpoint + "health"
	// quitPath is the quit endpoint.
	quitPath = adminEndpoint + "quit"
)

// An actionHandler is an interface which provides Get, Put & Delete
// to satisfy administrative REST APIs.
type actionHandler interface {
	Put(path string, body []byte, r *http.Request) error
	Get(path string, r *http.Request) (body []byte, contentType string, err error)
	Delete(path string, r *http.Request) error
}

// A adminServer provides a RESTful HTTP API to administration of
// the cockroach cluster.
type adminServer struct {
	db      *client.DB    // Key-value database client
	stopper *stop.Stopper // Used to shutdown the server
	mux     *http.ServeMux
}

// newAdminServer allocates and returns a new REST server for
// administrative APIs.
func newAdminServer(db *client.DB, stopper *stop.Stopper) *adminServer {
	server := &adminServer{
		db:      db,
		stopper: stopper,
		mux:     http.NewServeMux(),
	}

	server.mux.HandleFunc(debugEndpoint, server.handleDebug)
	server.mux.HandleFunc(healthPath, server.handleHealth)
	server.mux.HandleFunc(quitPath, server.handleQuit)
	return server
}

// ServeHTTP implements http.Handler.
func (s *adminServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

// handleHealth responds to health requests from monitoring services.
func (s *adminServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set(util.ContentTypeHeader, util.PlaintextContentType)
	fmt.Fprintln(w, "ok")
}

// handleQuit is the shutdown hook. The server is first placed into a
// draining mode, followed by exit.
func (s *adminServer) handleQuit(w http.ResponseWriter, r *http.Request) {
	w.Header().Set(util.ContentTypeHeader, util.PlaintextContentType)
	fmt.Fprintln(w, "ok")
	go func() {
		time.Sleep(50 * time.Millisecond)
		s.stopper.Stop()
	}()
}

// handleDebug passes requests with the debugPathPrefix onto the default
// serve mux, which is preconfigured (by import of expvar and net/http/pprof)
// to serve endpoints which access exported variables and pprof tools.
func (s *adminServer) handleDebug(w http.ResponseWriter, r *http.Request) {
	handler, _ := http.DefaultServeMux.Handler(r)
	handler.ServeHTTP(w, r)
}
