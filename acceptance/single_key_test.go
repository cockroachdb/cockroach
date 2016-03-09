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

package acceptance

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/acceptance/cluster"
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/timeutil"
)

// TestSingleKey stresses the transaction retry machinery by starting
// up an N node cluster and running N workers that are all
// incrementing the value associated with a single key.
func TestSingleKey(t *testing.T) {
	runTestOnConfigs(t, testSingleKeyInner)
}

func testSingleKeyInner(t *testing.T, c cluster.Cluster, cfg cluster.TestConfig) {
	num := c.NumNodes()

	// Initialize the value for our test key to zero.
	const key = "test-key"
	initDB, initDBStopper := c.NewClient(t, 0)
	defer initDBStopper.Stop()
	if err := initDB.Put(key, 0); err != nil {
		t.Fatal(err)
	}

	type result struct {
		err        error
		maxLatency time.Duration
	}

	resultCh := make(chan result, num)
	deadline := timeutil.Now().Add(cfg.Duration)
	var expected int64

	// Start up num workers each reading and writing the same
	// key. Each worker is configured to talk to a different node in the
	// cluster.
	for i := 0; i < num; i++ {
		db, dbStopper := c.NewClient(t, i)
		defer dbStopper.Stop()
		go func() {
			var r result
			for timeutil.Now().Before(deadline) {
				start := timeutil.Now()
				pErr := db.Txn(func(txn *client.Txn) *roachpb.Error {
					minExp := atomic.LoadInt64(&expected)
					r, pErr := txn.Get(key)
					if pErr != nil {
						return pErr
					}
					b := txn.NewBatch()
					v := r.ValueInt()
					b.Put(key, v+1)
					pErr = txn.CommitInBatch(b)
					// Atomic updates after the fact mean that we should read
					// exp or larger (since concurrent writers might have
					// committed but not yet performed their atomic update).
					if pErr == nil && v < minExp {
						return roachpb.NewErrorf("unexpected read: %d, expected >= %d", v, minExp)
					}
					return pErr
				})
				if pErr != nil {
					resultCh <- result{err: pErr.GoError()}
					return
				}
				atomic.AddInt64(&expected, 1)
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
	r, err := initDB.Get(key)
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
