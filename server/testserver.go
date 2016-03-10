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
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package server

import (
	"fmt"
	"net"
	"path/filepath"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/ts"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/metric"
	"github.com/cockroachdb/cockroach/util/retry"
	"github.com/cockroachdb/cockroach/util/stop"
)

const (
	// TestUser is a fixed user used in unittests.
	// It has valid embedded client certs.
	TestUser = "testuser"
	// initialSplitsTimeout is the amount of time to wait for initial splits to
	// occur on a freshly started server.
	// Note: this needs to be fairly high or tests become flaky.
	initialSplitsTimeout = 10 * time.Second
)

// StartTestServerWithContext starts an in-memory test server.
// ctx can be nil, in which case a default context will be created.
func StartTestServerWithContext(t util.Tester, ctx *Context) *TestServer {
	s := &TestServer{Ctx: ctx}
	if err := s.Start(); err != nil {
		if t != nil {
			t.Fatalf("Could not start server: %v", err)
		} else {
			log.Fatalf("Could not start server: %v", err)
		}
	}
	return s
}

// StartTestServer starts an in-memory test server.
func StartTestServer(t util.Tester) *TestServer {
	return StartTestServerWithContext(t, nil)
}

// StartTestServerJoining starts an in-memory test server that attempts to join `other`.
func StartTestServerJoining(t util.Tester, other *TestServer) *TestServer {
	s := &TestServer{Ctx: NewTestContext()}
	s.Ctx.JoinUsing = other.ServingAddr()
	if err := s.Start(); err != nil {
		if t != nil {
			t.Fatalf("Could not start server: %v", err)
		} else {
			log.Fatalf("Could not start server: %v", err)
		}
	}
	log.Infof("Node ID: %d", s.Gossip().GetNodeID())
	return s
}

// StartInsecureTestServer starts an insecure in-memory test server.
func StartInsecureTestServer(t util.Tester) *TestServer {
	s := &TestServer{Ctx: NewTestContext()}
	s.Ctx.Insecure = true

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
	ctx.MaxOffset = 50 * time.Millisecond

	// Test servers start in secure mode by default.
	ctx.Insecure = false

	// Load test certs. In addition, the tests requiring certs
	// need to call security.SetReadFileFn(securitytest.Asset)
	// in their init to mock out the file system calls for calls to AssetFS,
	// which has the test certs compiled in. Typically this is done
	// once per package, in main_test.go.
	ctx.SSLCA = filepath.Join(security.EmbeddedCertsDir, security.EmbeddedCACert)
	ctx.SSLCert = filepath.Join(security.EmbeddedCertsDir, security.EmbeddedNodeCert)
	ctx.SSLCertKey = filepath.Join(security.EmbeddedCertsDir, security.EmbeddedNodeKey)

	// Addr defaults to localhost with port set at time of call to
	// Start() to an available port.
	// Call TestServer.ServingAddr() for the full address (including bound port).
	ctx.Addr = "127.0.0.1:0"
	ctx.HTTPAddr = "127.0.0.1:0"
	// Set standard user for intra-cluster traffic.
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
	Ctx *Context
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

// RPCContext returns the rpc context used by the TestServer.
func (ts *TestServer) RPCContext() *rpc.Context {
	if ts != nil {
		return ts.rpcContext
	}
	return nil
}

// TsDB returns the ts.DB instance used by the TestServer.
func (ts *TestServer) TsDB() *ts.DB {
	if ts != nil {
		return ts.tsDB
	}
	return nil
}

// DB returns the client.DB instance used by the TestServer.
func (ts *TestServer) DB() *client.DB {
	if ts != nil {
		return ts.db
	}
	return nil
}

// Start starts the TestServer by bootstrapping an in-memory store
// (defaults to maximum of 100M). The server is started, launching the
// node RPC server and all HTTP endpoints. Use the value of
// TestServer.ServingAddr() after Start() for client connections. Use Stop()
// to shutdown the server after the test completes.
func (ts *TestServer) Start() error {
	return ts.StartWithStopper(nil)
}

// StartWithStopper is the same as Start, but allows passing a stopper
// explicitly.
func (ts *TestServer) StartWithStopper(stopper *stop.Stopper) error {
	if ts.Ctx == nil {
		ts.Ctx = NewTestContext()
	}

	if stopper == nil {
		stopper = stop.NewStopper()
	}

	// Change the replication requirements so we don't get log spam about ranges
	// not being replicated enough.
	cfg := config.DefaultZoneConfig()
	cfg.ReplicaAttrs = []roachpb.Attributes{{}}
	fn := config.TestingSetDefaultZoneConfig(cfg)
	stopper.AddCloser(stop.CloserFn(fn))

	// Needs to be called before NewServer to ensure resolvers are initialized.
	if err := ts.Ctx.InitNode(); err != nil {
		return err
	}

	var err error
	ts.Server, err = NewServer(ts.Ctx, stopper)
	if err != nil {
		return err
	}

	// Ensure we have the correct number of engines. Add in-memory ones where
	// needed. There must be at least one store/engine.
	if ts.StoresPerNode < 1 {
		ts.StoresPerNode = 1
	}
	for i := len(ts.Ctx.Engines); i < ts.StoresPerNode; i++ {
		ts.Ctx.Engines = append(ts.Ctx.Engines, engine.NewInMem(roachpb.Attributes{}, 100<<20, ts.Server.stopper))
	}
	if err := ts.Server.Start(); err != nil {
		return err
	}

	// If enabled, wait for initial splits to complete before returning control.
	// If initial splits do not complete, the server is stopped before
	// returning.
	if config.TestingTableSplitsDisabled() {
		return nil
	}
	if err := ts.WaitForInitialSplits(); err != nil {
		ts.Stop()
		return err
	}

	return nil
}

// ExpectedInitialRangeCount returns the expected number of ranges that should
// be on the server after initial (asynchronous) splits have been completed,
// assuming no additional information is added outside of the normal bootstrap
// process.
func ExpectedInitialRangeCount() int {
	return GetBootstrapSchema().DescriptorCount() - sql.NumSystemDescriptors + 1
}

// WaitForInitialSplits waits for the server to complete its expected initial
// splits at startup. If the expected range count is not reached within a
// configured timeout, an error is returned.
func (ts *TestServer) WaitForInitialSplits() error {
	expectedRanges := ExpectedInitialRangeCount()
	return util.RetryForDuration(initialSplitsTimeout, func() error {
		// Scan all keys in the Meta2Prefix; we only need a count.
		rows, pErr := ts.DB().Scan(keys.Meta2Prefix, keys.MetaMax, 0)
		if pErr != nil {
			return pErr.GoError()
		}
		if a, e := len(rows), expectedRanges; a != e {
			return util.Errorf("had %d ranges at startup, expected %d", a, e)
		}
		return nil
	})
}

// Stores returns the collection of stores from this TestServer's node.
func (ts *TestServer) Stores() *storage.Stores {
	return ts.node.stores
}

// ServingAddr returns the server's address. Should be used by clients.
func (ts *TestServer) ServingAddr() string {
	return ts.ctx.Addr
}

// HTTPAddr returns the server's HTTP address. Should be used by humans.
func (ts *TestServer) HTTPAddr() string {
	return ts.ctx.HTTPAddr
}

// ServingHost returns the host portion of the rpc server's address.
func (ts *TestServer) ServingHost() (string, error) {
	h, _, err := net.SplitHostPort(ts.ServingAddr())
	return h, err
}

// ServingPort returns the port portion of the rpc server's address.
func (ts *TestServer) ServingPort() (string, error) {
	_, p, err := net.SplitHostPort(ts.ServingAddr())
	return p, err
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
	if err := ts.node.stores.VisitStores(func(s *storage.Store) error {
		s.SetRangeRetryOptions(ro)
		return nil
	}); err != nil {
		panic(err)
	}
}

// WriteSummaries records summaries of time-series data, which is required for any tests
// that query server stats.
func (ts *TestServer) WriteSummaries() error {
	return ts.node.writeSummaries()
}

// MustGetSQLCounter returns the value of a counter metric from the server's SQL
// Executor. Runs in O(# of metrics) time, which is fine for test code.
func (ts *TestServer) MustGetSQLCounter(name string) int64 {
	var c int64
	var found bool

	ts.sqlExecutor.Registry().Each(func(n string, v interface{}) {
		if name == n {
			c = v.(*metric.Counter).Count()
			found = true
		}
	})
	if !found {
		panic(fmt.Sprintf("couldn't find metric %s", name))
	}
	return c
}

// MustGetSQLNetworkCounter returns the value of a counter metric from the
// server's SQL server. Runs in O(# of metrics) time, which is fine for test
// code.
func (ts *TestServer) MustGetSQLNetworkCounter(name string) int64 {
	var c int64
	var found bool

	ts.pgServer.Registry().Each(func(n string, v interface{}) {
		if name == n {
			c = v.(*metric.Counter).Count()
			found = true
		}
	})
	if !found {
		panic(fmt.Sprintf("couldn't find metric %s", name))
	}
	return c
}
