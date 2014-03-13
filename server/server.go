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
// Author: Andrew Bonventre (andybons@gmail.com)

// Package server implements a basic HTTP server for interacting with a node.
package server

import (
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"

	"github.com/cockroachdb/cockroach/db"
	"github.com/goraft/raft"
)

const (
	// dbKeyPrefix is the prefix for all RESTful endpoints used to
	// interact with the keyspace.
	dbKeyPrefix = "/db/"
	// raftBasePath is the prefix for all endpoints used to communicate Raft
	// consensus state with other nodes.
	raftBasePath = "/raft/"
)

var (
	addr        = flag.String("addr", "localhost:8080", "TCP network address to bind to.")
	raftLogPath = flag.String("raft_log_path", "./raft", "Path to write consensus state logs to.")
	raftName    = flag.String("name", "raftServer", "Name of the server to be used by raft.")
)

// ListenAndServe listens on the TCP network address specified by the
// addr flag and then calls ServeHTTP on a cockroach server to handle
// requests on incoming connections.
func ListenAndServe() error {
	return http.ListenAndServe(*addr, new())
}

type server struct {
	mux        *http.ServeMux
	db         db.DB
	raftServer raft.Server
}

func new() *server {
	s := &server{
		mux: http.NewServeMux(),
		db:  db.NewBasicDB(),
	}
	s.init()
	return s
}

func (s *server) init() {
	s.mux.HandleFunc("/healthz", s.handleHealthz)
	s.mux.HandleFunc(dbKeyPrefix, s.handleDBAction)
	s.setupRaftServer()
	log.Println("Started HTTP server at", *addr)
}

func (s *server) setupRaftServer() {
	transporter := raft.NewHTTPTransporter(raftBasePath)
	var err error
	// TODO: Pass a non-nil StateMachine to enable snapshotting and log compaction.
	s.raftServer, err = raft.NewServer(*raftName, *raftLogPath, transporter, nil, s.db, *addr)
	if err != nil {
		log.Fatal(err)
	}
	transporter.Install(s.raftServer, s.mux)
	s.raftServer.Start()
}

type gzipResponseWriter struct {
	io.WriteCloser
	http.ResponseWriter
}

func newGzipResponseWriter(w http.ResponseWriter) *gzipResponseWriter {
	gz := gzip.NewWriter(w)
	return &gzipResponseWriter{WriteCloser: gz, ResponseWriter: w}
}

func (w gzipResponseWriter) Write(b []byte) (int, error) {
	return w.WriteCloser.Write(b)
}

// ServeHTTP is necessary to implement the http.Handler interface. It
// will gzip a response if the appropriate request headers are set.
func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
		s.mux.ServeHTTP(w, r)
		return
	}
	w.Header().Set("Content-Encoding", "gzip")
	gzw := newGzipResponseWriter(w)
	defer gzw.Close()
	s.mux.ServeHTTP(gzw, r)
}

func (s *server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintln(w, "ok")
}

func (s *server) handleDBAction(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		s.handleDBGetAction(w, r)
	case "PUT", "POST":
		s.handleDBPutAction(w, r)
	case "DELETE":
		s.handleDBDeleteAction(w, r)
	default:
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
	}
}

func dbKey(path string) (string, error) {
	return url.QueryUnescape(strings.TrimPrefix(path, dbKeyPrefix))
}

func (s *server) handleDBPutAction(w http.ResponseWriter, r *http.Request) {
	key, err := dbKey(r.URL.Path)
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
	s.db.Put(key, string(b))
	w.WriteHeader(http.StatusOK)
}

func (s *server) handleDBGetAction(w http.ResponseWriter, r *http.Request) {
	key, err := dbKey(r.URL.Path)
	log.Println("Key:", key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	val := s.db.Get(key)
	if val == nil {
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "%+v", s.db.Get(key))
}

func (s *server) handleDBDeleteAction(w http.ResponseWriter, r *http.Request) {
	key, err := dbKey(r.URL.Path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.db.Delete(key)
	w.WriteHeader(http.StatusOK)
}
