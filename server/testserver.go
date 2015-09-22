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
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/ts"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/retry"
	"github.com/cockroachdb/cockroach/util/stop"
)

const (
	// TestUser is a fixed user used in unittests.
	// It has valid embedded client certs.
	TestUser = "testuser"
)

// StartTestServer starts a in-memory test server.
func StartTestServer(t util.Tester) *TestServer {
	s := &TestServer{}
	if err := s.Start(); err != nil {
		if t != nil {
			t.Fatalf("Could not start server: %v", err)
		} else {
			log.Fatalf("Could not start server: %v", err)
		}
	}

	return s
}

// NewTestContext returns a context for testing. It overrides the
// Certs with the test certs directory.
// We need to override the certs loader.
func NewTestContext() *Context {
	ctx := NewContext()

	// MaxOffset is the maximum offset for clocks in the cluster.
	// This is mostly irrelevant except when testing reads within
	// uncertainty intervals.
	ctx.MaxOffset = 0

	// Load test certs. In addition, the tests requiring certs
	// need to call security.SetReadFileFn(securitytest.Asset)
	// in their init to mock out the file system calls for calls to AssetFS,
	// which has the test certs compiled in. Typically this is done
	// once per package, in main_test.go.
	ctx.Certs = security.EmbeddedCertsDir
	// Addr defaults to localhost with port set at time of call to
	// Start() to an available port.
	// Call TestServer.ServingAddr() for the full address (including bound port).
	ctx.Addr = "127.0.0.1:0"
	// Set standard "node" user for intra-cluster traffic.
	ctx.User = security.NodeUser

	return ctx
}

// A TestServer encapsulates an in-memory instantiation of a cockroach
// node with a single store. Example usage of a TestServer follows:
//
//   s := server.StartTestServer(t)
//   defer s.Stop()
//
type TestServer struct {
	// Ctx is the context used by this server.
	Ctx           *Context
	SkipBootstrap bool
	// server is the embedded Cockroach server struct.
	*Server
	StoresPerNode int
}

// Stopper returns the embedded server's Stopper.
func (ts *TestServer) Stopper() *stop.Stopper {
	return ts.stopper
}

// Gossip returns the gossip instance used by the TestServer.
func (ts *TestServer) Gossip() *gossip.Gossip {
	if ts != nil {
		return ts.gossip
	}
	return nil
}

// Clock returns the clock used by the TestServer.
func (ts *TestServer) Clock() *hlc.Clock {
	if ts != nil {
		return ts.clock
	}
	return nil
}

//TsDB returns the ts.DB instance used by the TestServer.
func (ts *TestServer) TsDB() *ts.DB {
	if ts != nil {
		return ts.tsDB
	}
	return nil
}

// EventFeed returns the event feed that the server uses to publish events.
func (ts *TestServer) EventFeed() *util.Feed {
	if ts != nil {
		return ts.node.ctx.EventFeed
	}
	return nil
}

// Start starts the TestServer by bootstrapping an in-memory store
// (defaults to maximum of 100M). The server is started, launching the
// node RPC server and all HTTP endpoints. Use the value of
// TestServer.ServingAddr() after Start() for client connections. Use Stop()
// to shutdown the server after the test completes.
func (ts *TestServer) Start() error {
	if ts.Ctx == nil {
		ts.Ctx = NewTestContext()
	}

	var err error
	ts.Server, err = NewServer(ts.Ctx, stop.NewStopper())
	if err != nil {
		return err
	}

	// Ensure we have the correct number of engines. Add in in-memory ones where
	// needed.  There must be at least one store/engine.
	if ts.StoresPerNode < 1 {
		ts.StoresPerNode = 1
	}
	for i := len(ts.Ctx.Engines); i < ts.StoresPerNode; i++ {
		ts.Ctx.Engines = append(ts.Ctx.Engines, engine.NewInMem(proto.Attributes{}, 100<<20, ts.Server.stopper))
	}

	if !ts.SkipBootstrap {
		stopper := stop.NewStopper()
		_, err := BootstrapCluster("cluster-1", ts.Ctx.Engines, stopper)
		if err != nil {
			return util.Errorf("could not bootstrap cluster: %s", err)
		}
		stopper.Stop()
	}
	if err := ts.Server.Start(true); err != nil {
		return err
	}

	// TODO(marc): set this in the zones table when we have an entry
	// for the default cluster-wide zone config.
	config.DefaultZoneConfig.ReplicaAttrs = []proto.Attributes{{}}
	return nil
}

// ServingAddr returns the rpc server's address. Should be used by clients.
func (ts *TestServer) ServingAddr() string {
	return ts.rpc.Addr().String()
}

// Stop stops the TestServer.
func (ts *TestServer) Stop() {
	if r := recover(); r != nil {
		panic(r)
	}
	ts.Server.Stop()
}

// SetRangeRetryOptions sets the retry options for stores in TestServer.
func (ts *TestServer) SetRangeRetryOptions(ro retry.Options) {
	if err := ts.node.lSender.VisitStores(func(s *storage.Store) error {
		s.SetRangeRetryOptions(ro)
		return nil
	}); err != nil {
		panic(err)
	}
}
