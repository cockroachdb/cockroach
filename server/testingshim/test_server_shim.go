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

package testingshim

import "github.com/cockroachdb/cockroach/base"

// TestServerInterface defines test server functionality that tests need; it is
// implemented by server.TestServer.
type TestServerInterface interface {
	Start() error
	Stop()

	// ServingAddr returns the server's address.
	ServingAddr() string

	// ClientDB() returns a *client.DB instance for talking to this server, as
	// an interface{}.
	ClientDB() interface{}

	// KVDB() returns the *kv.DB instance as an interface{}.
	KVDB() interface{}
}

// TestServerParams contains the parameters we can set when creating a test
// server.
type TestServerParams struct {
	// Knobs for the test server.
	Knobs base.TestingKnobs

	// JoinAddr (if nonempty) is the address of a node we are joining.
	JoinAddr string
}

// TestServerFactory encompasses the actual implementation of the shim
// service.
type TestServerFactory interface {
	// New instantiates a test server instance.
	New(ctx TestServerParams) TestServerInterface
}

var serviceImpl TestServerFactory

// InitTestServerFactory should be called once to provide the implementation
// of the service. It will be called from a xx_test package that can import the
// server package.
func InitTestServerFactory(impl TestServerFactory) {
	serviceImpl = impl
}

// NewTestServerWithParams creates a new test server with the given parameters.
func NewTestServerWithParams(params TestServerParams) TestServerInterface {
	return serviceImpl.New(params)
}

// NewTestServer creates a new test server with default parameters.
func NewTestServer() TestServerInterface {
	return NewTestServerWithParams(TestServerParams{})
}
