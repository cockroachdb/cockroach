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
// Author: Bram Gruneir (bram.gruneir@gmail.com)

package server

import (
	// This is imported for its side-effect of registering expvar
	// endpoints with the http.DefaultServeMux.
	_ "expvar"
	"fmt"
	"io/ioutil"
	"net/http"
	// This is imported for its side-effect of registering pprof
	// endpoints with the http.DefaultServeMux.
	_ "net/http/pprof"
	"net/url"
	"strings"

	"github.com/cockroachdb/cockroach/storage"
)

const (
	maxGetResults = 0 // TODO(spencer): maybe we need paged query support

	// acctKeyPrefix is the prefix for accounting configuration changes.
	acctKeyPrefix = adminKeyPrefix + "acct"
	// adminKeyPrefix is the prefix for RESTful endpoints used to
	// provide an administrative interface to the cockroach cluster.
	adminKeyPrefix = "/_admin/"
	// debugKeyPrefix is the prefix of golang's standard debug functionality
	// for access to exported vars and pprof tools.
	debugKeyPrefix = "/debug/"
	// healthzKey is the healthz endpoint.
	healthzKey = adminKeyPrefix + "healthz"
	// permKeyPrefix is the prefix for permission configuration changes.
	permKeyPrefix = adminKeyPrefix + "perms"
	// zoneKeyPrefix is the prefix for zone configuration changes.
	zoneKeyPrefix = adminKeyPrefix + "zones"
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
	db   storage.DB // Key-value database client
	acct *acctHandler
	perm *permHandler
	zone *zoneHandler
}

// newAdminServer allocates and returns a new REST server for
// administrative APIs.
func newAdminServer(db storage.DB) *adminServer {
	return &adminServer{
		db:   db,
		acct: &acctHandler{db: db},
		perm: &permHandler{db: db},
		zone: &zoneHandler{db: db},
	}
}

// RegisterHandlers registers admin handlers with the supplied
// serve mux.
func (s *adminServer) RegisterHandlers(mux *http.ServeMux) {
	// Pass through requests to /debug to the default serve mux so we
	// get exported variables and pprof tools.
	mux.HandleFunc(acctKeyPrefix, s.handleAcctAction)
	mux.HandleFunc(acctKeyPrefix+"/", s.handleAcctAction)
	mux.HandleFunc(debugKeyPrefix, s.handleDebug)
	mux.HandleFunc(healthzKey, s.handleHealthz)
	mux.HandleFunc(permKeyPrefix, s.handlePermAction)
	mux.HandleFunc(permKeyPrefix+"/", s.handlePermAction)
	mux.HandleFunc(zoneKeyPrefix, s.handleZoneAction)
	mux.HandleFunc(zoneKeyPrefix+"/", s.handleZoneAction)
}

// handleHealthz responds to health requests from monitoring services.
func (s *adminServer) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintln(w, "ok")
}

// handleDebug passes requests with the debugKeyPrefix onto the default
// serve mux, which is preconfigured (by import of expvar and net/http/pprof)
// to serve endpoints which access exported variables and pprof tools.
func (s *adminServer) handleDebug(w http.ResponseWriter, r *http.Request) {
	handler, _ := http.DefaultServeMux.Handler(r)
	handler.ServeHTTP(w, r)
}

// TODO(bram): using a single handler instead of one each for zone/perm/acct
// handleAcctAction handles actions for accounting configuration by method.
func (s *adminServer) handleAcctAction(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		s.handleGetAction(s.acct, w, r, acctKeyPrefix)
	case "PUT", "POST":
		s.handlePutAction(s.acct, w, r, acctKeyPrefix)
	case "DELETE":
		s.handleDeleteAction(s.acct, w, r, acctKeyPrefix)
	default:
		http.Error(w, "Bad Request", http.StatusBadRequest)
	}
}

// handlePermAction handles actions for perm configuration by method.
func (s *adminServer) handlePermAction(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		s.handleGetAction(s.perm, w, r, permKeyPrefix)
	case "PUT", "POST":
		s.handlePutAction(s.perm, w, r, permKeyPrefix)
	case "DELETE":
		s.handleDeleteAction(s.perm, w, r, permKeyPrefix)
	default:
		http.Error(w, "Bad Request", http.StatusBadRequest)
	}
}

// handleZoneAction handles actions for zone configuration by method.
func (s *adminServer) handleZoneAction(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		s.handleGetAction(s.zone, w, r, zoneKeyPrefix)
	case "PUT", "POST":
		s.handlePutAction(s.zone, w, r, zoneKeyPrefix)
	case "DELETE":
		s.handleDeleteAction(s.zone, w, r, zoneKeyPrefix)
	default:
		http.Error(w, "Bad Request", http.StatusBadRequest)
	}
}

func unescapePath(path, prefix string) (string, error) {
	result, err := url.QueryUnescape(strings.TrimPrefix(path, prefix))
	if err != nil {
		return "", err
	}
	return result, nil
}

func (s *adminServer) handlePutAction(handler actionHandler, w http.ResponseWriter, r *http.Request, prefix string) {
	path, err := unescapePath(r.URL.Path, prefix)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()
	if err = handler.Put(path, b, r); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *adminServer) handleGetAction(handler actionHandler, w http.ResponseWriter, r *http.Request, prefix string) {
	path, err := unescapePath(r.URL.Path, prefix)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	b, contentType, err := handler.Get(path, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", contentType)
	fmt.Fprintf(w, "%s", string(b))
}

func (s *adminServer) handleDeleteAction(handler actionHandler, w http.ResponseWriter, r *http.Request, prefix string) {
	path, err := unescapePath(r.URL.Path, prefix)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err = handler.Delete(path, r); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}
