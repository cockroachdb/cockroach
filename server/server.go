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
	"bytes"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
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
	addr         = flag.String("addr", "localhost:8080", "TCP network address to bind to.")
	raftDataPath = flag.String("raft_data_path", "./node.1", "Path to write consensus state logs to.")
	raftName     = flag.String("name", "raftServer", "Name of the server to be used by raft.")
	raftLeader   = flag.String("raft_leader", "", "The raft leader (host:port) to join.")
)

// ListenAndServe listens on the TCP network address specified by the
// addr flag and then calls ServeHTTP on a cockroach server to handle
// requests on incoming connections.
func ListenAndServe() error {
	s := newServer()
	s.init()
	return http.ListenAndServe(*addr, s)
}

type server struct {
	mux        *http.ServeMux
	db         db.DB
	raftServer raft.Server
}

func newServer() *server {
	return &server{
		mux: http.NewServeMux(),
		db:  db.NewBasicDB(),
	}
}

func (s *server) init() {
	log.Println("Starting HTTP server at", *addr)
	s.mux.HandleFunc("/join", s.handleRaftJoin)
	s.mux.HandleFunc("/healthz", s.handleHealthz)
	s.mux.HandleFunc(dbKeyPrefix, s.handleDBAction)
	s.setupRaftServer()
}

func (s *server) setupRaftServer() {
	if err := os.MkdirAll(*raftDataPath, 0744); err != nil {
		log.Fatalf("raft: unable to create path: %v", err)
	}
	transporter := raft.NewHTTPTransporter(raftBasePath)
	var err error
	// TODO: Pass a non-nil StateMachine to enable snapshotting and log compaction.
	s.raftServer, err = raft.NewServer(*raftName, *raftDataPath, transporter, nil, s.db, *addr)
	if err != nil {
		log.Fatal(err)
	}
	transporter.Install(s.raftServer, s.mux)
	log.Println("raft: starting server...")
	s.raftServer.Start()
	if s.raftServer.IsLogEmpty() {
		leader := *raftLeader
		if len(leader) == 0 {
			// Join self to create a new cluster.
			leader = *addr
		}
		if err := s.joinRaftLeader(leader); err != nil {
			log.Printf("raft: Unable to join leader %s: %s", leader, err)
		}
	} else {
		log.Println("raft: recovering from log...")
	}
}

func (s *server) joinRaftLeader(leader string) error {
	log.Println("raft: joining leader", leader)
	cmd := &raft.DefaultJoinCommand{
		Name:             s.raftServer.Name(),
		ConnectionString: "http://" + *addr, // TODO(andybons): TLS.
	}
	if leader == *addr {
		log.Println("raft: starting a new cluster...")
		if _, err := s.raftServer.Do(cmd); err != nil {
			log.Fatal(err)
		}
		return nil
	}
	b := &bytes.Buffer{}
	if err := json.NewEncoder(b).Encode(cmd); err != nil {
		return err
	}
	log.Println("Attempting to join leader via HTTP:", fmt.Sprintf("http://%s/join", leader))
	resp, err := http.Post(fmt.Sprintf("http://%s/join", leader), "application/json", b)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return fmt.Errorf("raft: got non-200 status code: %d. response: %s", resp.StatusCode, string(b))
	}
	return nil
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
	// Execute the command against the Raft server.
	if _, err := s.raftServer.Do(db.NewPutCommand(key, string(b))); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *server) handleDBGetAction(w http.ResponseWriter, r *http.Request) {
	key, err := dbKey(r.URL.Path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	val, err := s.db.Get(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if val == nil {
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "%+v", val)
}

func (s *server) handleDBDeleteAction(w http.ResponseWriter, r *http.Request) {
	key, err := dbKey(r.URL.Path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// Execute the command against the Raft server.
	if _, err := s.raftServer.Do(db.NewDeleteCommand(key)); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *server) handleRaftJoin(w http.ResponseWriter, r *http.Request) {
	cmd := &raft.DefaultJoinCommand{}
	if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if _, err := s.raftServer.Do(cmd); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
