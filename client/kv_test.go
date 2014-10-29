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

package client_test

import "testing"

// TestKVRetryOnWrite verifies that the client will retry as
// appropriate on write/write conflicts.
func TestKVRetryOnWrite(t *testing.T) {
	// Start a transaction
}

// TestKVRetryOnRead verifies that the client will retry on
// read/write conflicts.
func TestKVRetryOnRead(t *testing.T) {
}

// TestKVRunTransaction verifies some simple transaction isolation
// semantics.
func TestKVRunTransaction(t *testing.T) {
}

// TestKVNestedTransactions verifies that trying to create nested
// transactions returns an error.
func TestKVNestedTransactions(t *testing.T) {
}
