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
// Author: Radu Berinde (radu.berindeg@gmail.com)
//
// This file provides generic interfaces that allow tests to set up test servers
// without importing the server package (avoiding circular dependencies).
// To be used, the binary needs to call
// testingshim.InitTestServerFactory(server.TestServerFactory), generally from a
// TestMain() in an "foo_test" package (which can import server and is linked
// together with the other tests in package "foo").

package testingshim

import (
	"net/http"
	"time"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/stop"
)

// TestServerInterface defines test server functionality that tests need; it is
// implemented by server.TestServer.
type TestServerInterface interface {
	Stopper() *stop.Stopper

	Start(params TestServerParams) error

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

	// Clock returns the clock used by the TestServer.
	Clock() *hlc.Clock

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
}

// TestServerParams contains the parameters we can set when creating a test
// server.
// The zero value is suitable for most tests.
type TestServerParams struct {
	// Knobs for the test server.
	Knobs base.TestingKnobs

	// JoinAddr (if nonempty) is the address of a node we are joining.
	JoinAddr string

	StoresPerNode int

	// Fields copied to the server.Context.
	Insecure              bool
	MetricsSampleInterval time.Duration
	MaxOffset             time.Duration
	SocketFile            string
	ScanInterval          time.Duration
	ScanMaxIdleTime       time.Duration
	SSLCA                 string
	SSLCert               string
	SSLCertKey            string

	// If set, this will be appended to the Postgres URL by functions that
	// automatically open a connection to the server. That's equivalent to running
	// SET DATABASE=foo, which works even if the database doesn't (yet) exist.
	UseDatabase string

	// Stopper can be used to stop the server. If not set, a stopper will be
	// constructed and it can be gotten through TestServerInterface.Stopper().
	Stopper *stop.Stopper
}

// TestServerFactory encompasses the actual implementation of the shim
// service.
type TestServerFactory interface {
	// New instantiates a test server.
	New(params TestServerParams) TestServerInterface
}

var serviceImpl TestServerFactory

// InitTestServerFactory should be called once to provide the implementation
// of the service. It will be called from a xx_test package that can import the
// server package.
func InitTestServerFactory(impl TestServerFactory) {
	serviceImpl = impl
}

// StartServerRaw creates and starts a TestServer.
// Generally sqlutils.SetupServer() should be used. However this function can be
// used directly when opening a connection to the server is not desired.
func StartServerRaw(params TestServerParams) (TestServerInterface, error) {
	if serviceImpl == nil {
		panic("TestServerFactory not initialized. One needs to be injected " +
			"from the package's TestMain()")
	}
	server := serviceImpl.New(params)
	return server, server.Start(params)
}
