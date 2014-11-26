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

package server

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
)

const (
	defaultHTTPAddr = "127.0.0.1:0"
	defaultRPCAddr  = "127.0.0.1:0"
)

// A TestServer encapsulates an in-memory instantiation of a cockroach
// node with a single store. Example usage of a TestServer follows:
//
//   s := &server.TestServer{}
//   if err := s.Start(); err != nil {
//     t.Fatal(err)
//   }
//   defer s.Stop()
//
// TODO(spencer): add support for multiple stores.
type TestServer struct {
	// CertDir specifies the directory containing certs for SSL
	// connections. Default will load insecure TLS config.
	CertDir string
	// MaxOffset is the maximum offset for clocks in the cluster.
	// This is mostly irrelevant except when testing reads within
	// uncertainty intervals.
	MaxOffset time.Duration
	// HTTPAddr and RPCAddr default to localhost with port set
	// at time of call to Start() to an available port.
	HTTPAddr, RPCAddr string
	// server is the embedded Cockroach server struct.
	*server
}

// StartTestServer creates a TestServer instance; on failure, causes
// a fatal testing error. The new TestServer is returned on success.
func StartTestServer(t *testing.T) *TestServer {
	s := &TestServer{}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	return s
}

// Gossip returns the gossip instance used by the TestServer.
func (ts *TestServer) Gossip() *gossip.Gossip {
	return ts.gossip
}

// Start starts the TestServer by bootstrapping an in-memory store
// (defaults to maximum of 100M). The server is started, launching the
// node RPC server and all HTTP endpoints. Use the value of
// TestServer.HTTPAddr after Start() for client connections.
func (ts *TestServer) Start() error {
	// We update these with the actual port once the servers
	// have been launched for the purpose of this test.
	if ts.RPCAddr == "" {
		ts.RPCAddr = defaultRPCAddr
	}
	if ts.HTTPAddr == "" {
		ts.HTTPAddr = defaultHTTPAddr
	}
	var err error
	ts.server, err = newServer(ts.RPCAddr, ts.CertDir, ts.MaxOffset)
	if err != nil {
		return util.Errorf("could not init server: %s", err)
	}
	engines := []engine.Engine{engine.NewInMem(proto.Attributes{}, 100<<20)}
	if _, err := BootstrapCluster("cluster-1", engines[0]); err != nil {
		return util.Errorf("could not bootstrap cluster: %s", err)
	}
	err = ts.start(engines, "", ts.HTTPAddr, true) // TODO(spencer): should shutdown server.
	if err != nil {
		return util.Errorf("could not start server: %s", err)
	}
	// Update the configuration variables to reflect the actual
	// ports bound.
	ts.HTTPAddr = (*ts.httpListener).Addr().String()
	ts.RPCAddr = ts.rpc.Addr().String()

	return nil
}

// Stop stops the TestServer.
func (ts *TestServer) Stop() {
	ts.stop()
}
