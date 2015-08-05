// Copyright 2015 The Cockroach Authors.
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
// Author: Peter Mattis (peter@cockroachlabs.com)

// +build acceptance

package acceptance

import (
	"encoding"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/acceptance/localcluster"
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/util/log"
)

var _ encoding.BinaryMarshaler = testVal(0)
var _ encoding.BinaryUnmarshaler = new(testVal)

//
// TestSingleKeySingleNodeConcurrentTxn stresses the transaction
// retry machinery by starting up a numNodes node cluster
// and running numTrans workers that are all incrementing the value
// associated with a single key against one node.
func TestSingleKeySingleNodeConcurrentTxn(t *testing.T) {
	l := localcluster.Create(*numNodes, stopper)
	l.Start()
	defer l.Stop()

	checkRangeReplication(t, l, 20*time.Second)

	// Initialize the value for our test key to zero.
	const key = "test-key"
	db := makeDBClient(t, l, 0)
	if err := db.Put(key, testVal(0)); err != nil {
		t.Fatal(err)
	}

	type result struct {
		err        error
		count      int
		maxLatency time.Duration
	}

	resultCh := make(chan result, *numTrans)
	var expected int64

	// Start up numTrans workers each reading and writing the same
	// key. Each worker is configured to talk to the same node in the
	// cluster.
	for i := 0; i < *numTrans; i++ {
		go func() {
			var r result
			start := time.Now()
			err := db.Txn(func(txn *client.Txn) error {
				r, err := txn.Get(key)
				if err != nil {
					return err
				}
				var v testVal
				if err := v.UnmarshalBinary(r.ValueBytes()); err != nil {
					return err
				}
				b := &client.Batch{}
				b.Put(key, v+1)
				return txn.Commit(b)
			})
			if err != nil {
				resultCh <- result{err: err}
				return
			}
			atomic.AddInt64(&expected, 1)
			r.count++
			latency := time.Since(start)
			if r.maxLatency < latency {
				r.maxLatency = latency
			}
			resultCh <- r
		}()
	}

	// Verify that none of the workers encountered an error.
	var results []result
	for len(results) < *numTrans {
		select {
		case <-stopper:
			t.Fatalf("interrupted")
		case r := <-resultCh:
			if r.err != nil {
				t.Fatal(r.err)
			}
			results = append(results, r)
		case <-time.After(1 * time.Second):
			// Periodically print out progress so that we know the test is still
			// running.
			log.Infof("%d", atomic.LoadInt64(&expected))
		}
	}

	// Verify the resulting value stored at the key is what we expect.
	r, err := db.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	var v testVal
	if err := v.UnmarshalBinary(r.ValueBytes()); err != nil {
		t.Fatal(err)
	}
	if expected != int64(v) {
		t.Fatalf("expected %d, but found %d", expected, v)
	}
	var maxLatency []time.Duration
	for _, r := range results {
		maxLatency = append(maxLatency, r.maxLatency)
	}
	log.Infof("%d increments: %s", v, maxLatency)
}
