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
// permissions and limitations under the License.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

// +build acceptance

package acceptance

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/log"
)

// TestSingleKey stresses the transaction retry machinery by starting
// up an N node cluster and running N workers that are all
// incrementing the value associated with a single key.
func TestSingleKey(t *testing.T) {
	c := StartCluster(t)
	defer c.AssertAndStop(t)
	num := c.NumNodes()

	// Initialize the value for our test key to zero.
	const key = "test-key"
	db, initDBStopper := makeClient(t, c.ConnString(0))
	defer initDBStopper.Stop()
	if err := db.Put(key, 0); err != nil {
		t.Fatal(err)
	}

	type result struct {
		err        error
		count      int
		maxLatency time.Duration
	}

	resultCh := make(chan result, num)
	deadline := time.Now().Add(*duration)
	var expected int64

	// Start up num workers each reading and writing the same
	// key. Each worker is configured to talk to a different node in the
	// cluster.
	for i := 0; i < num; i++ {
		db, dbStopper := makeClient(t, c.ConnString(i))
		defer dbStopper.Stop()
		go func() {
			var r result
			for time.Now().Before(deadline) {
				start := time.Now()
				pErr := db.Txn(func(txn *client.Txn) *roachpb.Error {
					r, pErr := txn.Get(key)
					if pErr != nil {
						return pErr
					}
					b := txn.NewBatch()
					b.Put(key, r.ValueInt()+1)
					return txn.CommitInBatch(b)
				})
				if pErr != nil {
					resultCh <- result{err: pErr.GoError()}
					return
				}
				atomic.AddInt64(&expected, 1)
				r.count++
				latency := time.Since(start)
				if r.maxLatency < latency {
					r.maxLatency = latency
				}
			}
			resultCh <- r
		}()
	}

	// Verify that none of the workers encountered an error.
	var results []result
	for len(results) < num {
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
	v := r.ValueInt()
	if expected != v {
		t.Fatalf("expected %d, but found %d", expected, v)
	}
	var maxLatency []time.Duration
	for _, r := range results {
		maxLatency = append(maxLatency, r.maxLatency)
	}
	log.Infof("%d increments: %s", v, maxLatency)
}
