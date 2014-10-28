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

// ExampleKVRunTransaction shows an example of using the KV client's
// RunTransaction method.
func ExampleKVRunTransaction() {
	/*
			if err := client.RunTransaction(txnOptions, func(txn *client.KV) error {
		    getReply := &proto.GetResponse{}
		    txn.Call(proto.Get, &proto.GetRequest{RequestHeader: proto.RequestHeader{Key: key}}, getReply)

		    // Work some magic with existing value to create 10 new values and
		    // write them all in parallel.
		    calls := make([]*client.Call, 10)
		    for i := 0; i < 10; i++ {
		      key := proto.Key(fmt.Sprintf("key-%d", i))
		      value := proto.Value{Bytes: fmt.Sprintf("%d-%q", i, getReply.Value.Bytes)}
		      req := &proto.PutRequest{RequestHeader: proto.RequestHeader{Key: key}, Value: value}
		      calls[i] = txn.Go(proto.Put, req, &proto.PutResponse{}, nil)
		    }

		    // We don't have to wait for all calls to complete; transaction commit will do so automatically.
		  }); err != nil {
		    log.Errorf("txn failed: %s", err)
		  }
	*/
	// Output:
}

// TestKVRetryOnWrite verifies that the client will retry as
// appropriate on write/write conflicts.
func TestKVRetryOnWrite(t *testing.T) {
}

// TestKVRetryOnRead verifies that the client will retry on
// read/write conflicts.
func TestKVRetryOnRead(t *testing.T) {
}

// TestKVRunTransaction verifies some simple transaction isolation
// semantics.
func TestKVRunTransaction(t *testing.T) {
}
