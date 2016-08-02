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
// This file provides generic interfaces that allow tests to set up test
// clusters without importing the testcluster (and indirectly server) package
// (avoiding circular dependencies). To be used, the binary needs to call
// InitTestClusterFactory(testcluster.TestClusterFactory), generally from a
// TestMain() in an "foo_test" package (which can import testcluster and is
// linked together with the other tests in package "foo").

package serverutils

import (
	gosql "database/sql"
	"testing"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/util/stop"
)

// TestClusterInterface defines TestCluster functionality used by tests.
type TestClusterInterface interface {
	NumServers() int

	// Server returns the TestServerInterface corresponding to a specific node.
	Server(idx int) TestServerInterface

	// ServerConn returns a gosql.DB connection to a specific node.
	ServerConn(idx int) *gosql.DB

	WaitForFullReplication() error

	// StopServer stops a single server.
	StopServer(idx int)

	// Stopper retrieves the stopper for this test cluster. Tests should call or
	// defer the Stop() method on this stopper after starting a test cluster.
	Stopper() *stop.Stopper
}

// TestClusterFactory encompasses the actual implementation of the shim
// service.
type TestClusterFactory interface {
	// New instantiates a test server.
	StartTestCluster(t testing.TB, numNodes int, args base.TestClusterArgs) TestClusterInterface
}

var clusterFactoryImpl TestClusterFactory

// InitTestClusterFactory should be called once to provide the implementation
// of the service. It will be called from a xx_test package that can import the
// server package.
func InitTestClusterFactory(impl TestClusterFactory) {
	clusterFactoryImpl = impl
}

// StartTestCluster starts up a TestCluster made up of numNodes in-memory
// testing servers. The cluster should be stopped using Stopper().Stop().
func StartTestCluster(t testing.TB, numNodes int, args base.TestClusterArgs) TestClusterInterface {
	if srvFactoryImpl == nil {
		panic("TestClusterFactory not initialized. One needs to be injected " +
			"from the package's TestMain()")
	}
	return clusterFactoryImpl.StartTestCluster(t, numNodes, args)
}
