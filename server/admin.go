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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package server

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	"github.com/cockroachdb/cockroach/kv"
)

const (
	// adminKeyPrefix is the prefix for RESTful endpoints used to
	// provide an administrative interface to the cockroach cluster.
	adminKeyPrefix = "/_admin/"
	// zoneKeyPrefix is the prefix for zone configuration changes.
	zoneKeyPrefix = adminKeyPrefix + "zones"
)

// A actionHandler is an interface which provides Get, Put & Delete
// to satisfy administrative REST APIs.
type actionHandler interface {
	Put(path string, body []byte, r *http.Request) error
	Get(path string, r *http.Request) (body []byte, contentType string, err error)
	Delete(path string, r *http.Request) error
}

// A adminServer provides a RESTful HTTP API to administration of
// the cockroach cluster.
type adminServer struct {
	kvDB kv.DB // Key-value database client
	zone *zoneHandler
}

// newAdminServer allocates and returns a new REST server for
// administrative APIs.
func newAdminServer(kvDB kv.DB) *adminServer {
	return &adminServer{
		kvDB: kvDB,
		zone: &zoneHandler{kvDB: kvDB},
	}
}

// handleHealthz responds to health requests from monitoring services.
func (s *adminServer) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintln(w, "ok")
}

// handleZoneAction handles actions for zone configuration by method.
func (s *adminServer) handleZoneAction(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		s.handleGetAction(s.zone, w, r)
	case "PUT", "POST":
		s.handlePutAction(s.zone, w, r)
	case "DELETE":
		s.handleDeleteAction(s.zone, w, r)
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

func (s *adminServer) handlePutAction(handler actionHandler, w http.ResponseWriter, r *http.Request) {
	path, err := unescapePath(r.URL.Path, zoneKeyPrefix)
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

func (s *adminServer) handleGetAction(handler actionHandler, w http.ResponseWriter, r *http.Request) {
	path, err := unescapePath(r.URL.Path, zoneKeyPrefix)
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

func (s *adminServer) handleDeleteAction(handler actionHandler, w http.ResponseWriter, r *http.Request) {
	path, err := unescapePath(r.URL.Path, zoneKeyPrefix)
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
