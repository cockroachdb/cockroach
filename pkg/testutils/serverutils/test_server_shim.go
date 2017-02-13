// Copyright 2016 The Cockroach Authors.
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
// Author: Radu Berinde (radu@cockroachlabs.com)
// Author: Andrei Matei (andreimatei1@gmail.com)
//
// This file provides generic interfaces that allow tests to set up test servers
// without importing the server package (avoiding circular dependencies).
// To be used, the binary needs to call
// InitTestServerFactory(server.TestServerFactory), generally from a TestMain()
// in an "foo_test" package (which can import server and is linked together with
// the other tests in package "foo").

package serverutils

import (
	gosql "database/sql"
	"net/http"
	"net/url"
	"testing"

	"github.com/gogo/protobuf/proto"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// TestServerInterface defines test server functionality that tests need; it is
// implemented by server.TestServer.
type TestServerInterface interface {
	Stopper() *stop.Stopper

	Start(params base.TestServerArgs) error

	// NodeID returns the ID of this node within its cluster.
	NodeID() roachpb.NodeID

	// ServingAddr returns the server's address.
	ServingAddr() string

	// KVClient() returns a *client.DB instance for talking to this KV server,
	// as an interface{}.
	KVClient() interface{}

	// KVDB() returns the *kv.DB instance as an interface{}.
	KVDB() interface{}

	// RPCContext returns the rpc context used by the test server.
	RPCContext() *rpc.Context

	// LeaseManager() returns the *sql.LeaseManager as an interface{}.
	LeaseManager() interface{}

	// Gossip returns the gossip used by the TestServer.
	Gossip() *gossip.Gossip

	// Clock returns the clock used by the TestServer.
	Clock() *hlc.Clock

	// DistSender returns the DistSender used by the TestServer.
	DistSender() *kv.DistSender

	// DistSQLServer returns the *distsqlrun.ServerImpl as an interface{}.
	DistSQLServer() interface{}

	// SetDistSQLSpanResolver changes the SpanResolver used for DistSQL inside the
	// server's executor. The argument must be a distsqlplan.SpanResolver
	// instance.
	//
	// This method exists because we cannot pass the fake span resolver with the
	// server or cluster params: the fake span resolver needs the node IDs and
	// addresses of the servers in a cluster, which are not available before we
	// start the servers.
	//
	// It is the caller's responsibility to make sure no queries are being run
	// with DistSQL at the same time.
	SetDistSQLSpanResolver(spanResolver interface{})

	// AdminURL returns the URL for the admin UI.
	AdminURL() string
	// GetHTTPClient returns an http client connected to the server. It uses the
	// context client TLS config.
	GetHTTPClient() (http.Client, error)

	// MustGetSQLCounter returns the value of a counter metric from the server's
	// SQL Executor. Runs in O(# of metrics) time, which is fine for test code.
	MustGetSQLCounter(name string) int64
	// MustGetSQLNetworkCounter returns the value of a counter metric from the
	// server's SQL server. Runs in O(# of metrics) time, which is fine for test
	// code.
	MustGetSQLNetworkCounter(name string) int64
	// WriteSummaries records summaries of time-series data, which is required for
	// any tests that query server stats.
	WriteSummaries() error

	// GetFirstStoreID is a utility function returning the StoreID of the first
	// store on this node.
	GetFirstStoreID() roachpb.StoreID

	// SplitRange splits the range containing splitKey.
	SplitRange(
		splitKey roachpb.Key,
	) (left roachpb.RangeDescriptor, right roachpb.RangeDescriptor, err error)
}

// TestServerFactory encompasses the actual implementation of the shim
// service.
type TestServerFactory interface {
	// New instantiates a test server.
	New(params base.TestServerArgs) interface{}
}

var srvFactoryImpl TestServerFactory

// InitTestServerFactory should be called once to provide the implementation
// of the service. It will be called from a xx_test package that can import the
// server package.
func InitTestServerFactory(impl TestServerFactory) {
	srvFactoryImpl = impl
}

// StartServer creates a test server and sets up a gosql DB connection.
// The server should be stopped by calling server.Stopper().Stop().
func StartServer(
	t testing.TB, params base.TestServerArgs,
) (TestServerInterface, *gosql.DB, *client.DB) {
	server, err := StartServerRaw(params)
	if err != nil {
		t.Fatal(err)
	}

	kvClient := server.KVClient().(*client.DB)
	pgURL, cleanupGoDB := sqlutils.PGUrl(
		t, server.ServingAddr(), "StartServer", url.User(security.RootUser))
	pgURL.Path = params.UseDatabase
	goDB, err := gosql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	server.Stopper().AddCloser(
		stop.CloserFn(func() {
			_ = goDB.Close()
			cleanupGoDB()
		}))
	return server, goDB, kvClient
}

// StartServerRaw creates and starts a TestServer.
// Generally StartServer() should be used. However this function can be used
// directly when opening a connection to the server is not desired.
func StartServerRaw(args base.TestServerArgs) (TestServerInterface, error) {
	if srvFactoryImpl == nil {
		panic("TestServerFactory not initialized. One needs to be injected " +
			"from the package's TestMain()")
	}
	server := srvFactoryImpl.New(args).(TestServerInterface)
	if err := server.Start(args); err != nil {
		return nil, err
	}
	return server, nil
}

// GetJSONProto uses the supplied client to GET the URL specified by the parameters
// and unmarshals the result into response.
func GetJSONProto(ts TestServerInterface, path string, response proto.Message) error {
	httpClient, err := ts.GetHTTPClient()
	if err != nil {
		return err
	}
	return httputil.GetJSON(httpClient, ts.AdminURL()+path, response)
}

// PostJSONProto uses the supplied client to POST request to the URL specified by
// the parameters and unmarshals the result into response.
func PostJSONProto(ts TestServerInterface, path string, request, response proto.Message) error {
	httpClient, err := ts.GetHTTPClient()
	if err != nil {
		return err
	}
	return httputil.PostJSON(httpClient, ts.AdminURL()+path, request, response)
}
